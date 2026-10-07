package connection

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aelpxy/pulse/protocol"
	"github.com/charmbracelet/log"
	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

var errSendBufferFull = errors.New("send buffer full")

// broadcast payloads are shared across subscribers, so this mostly bounds how far one client may lag
const maxQueuedBytes = 1 << 20

var frameBufPool = sync.Pool{New: func() any {
	b := make([]byte, 0, 16*1024)
	return &b
}}

type Connection struct {
	ID              string
	AppKey          string
	ws              *websocket.Conn
	sendMu          sync.Mutex
	sendQueue       [][]byte
	queuedBytes     int
	slowConsumer    bool
	sendReady       chan struct{}
	manager         *Manager
	channels        map[string]bool
	channelsMux     sync.RWMutex
	lastActivityNS  atomic.Int64
	activityTimeout time.Duration
	closing         chan struct{}
	closeMux        sync.Mutex
	isClosed        bool
	closeCode       int
	closeReason     string
	closeOnce       sync.Once
	rateLimiter     *rate.Limiter
	userID          string
	userMux         sync.RWMutex
}

func NewConnection(id string, ws *websocket.Conn, manager *Manager, activityTimeout time.Duration) *Connection {
	conn := &Connection{
		ID:              id,
		ws:              ws,
		sendReady:       make(chan struct{}, 1),
		manager:         manager,
		channels:        make(map[string]bool),
		activityTimeout: activityTimeout,
		closing:         make(chan struct{}),
		isClosed:        false,
		rateLimiter:     rate.NewLimiter(10, 20), // default, updated per app config
	}
	conn.touchActivity()
	return conn
}

func (c *Connection) SetRateLimit(eventsPerSecond int, burst int) {
	c.rateLimiter = rate.NewLimiter(rate.Limit(eventsPerSecond), burst)
}

func (c *Connection) UserID() string {
	c.userMux.RLock()
	defer c.userMux.RUnlock()
	return c.userID
}

func (c *Connection) setUserID(userID string) {
	c.userMux.Lock()
	c.userID = userID
	c.userMux.Unlock()
}

func (c *Connection) ReadPump() {
	defer func() {
		c.closeWebSocket()
		c.manager.Unregister(c)
	}()

	c.ws.SetReadDeadline(time.Now().Add(c.activityTimeout))

	c.ws.SetPongHandler(func(string) error {
		c.touchActivity()
		c.ws.SetReadDeadline(time.Now().Add(c.activityTimeout))
		return nil
	})

	for {
		_, message, err := c.ws.ReadMessage()
		if err != nil {
			if log.GetLevel() == log.DebugLevel {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Debug("websocket error", "connection", c.ID, "error", err)
				}
			}
			break
		}

		c.touchActivity()

		c.handleMessage(message)
	}
}

func (c *Connection) touchActivity() {
	c.lastActivityNS.Store(time.Now().UnixNano())
}

func (c *Connection) LastActivity() time.Time {
	ns := c.lastActivityNS.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (c *Connection) WritePump() {
	ticker := time.NewTicker(54 * time.Second)
	// unregister is left to ReadPump so it never races with subscribe handling
	defer func() {
		ticker.Stop()
		c.closeWebSocket()
	}()

	const writeDeadline = 10 * time.Second
	var spare [][]byte

	for {
		select {
		case <-c.sendReady:
			batch := c.takeQueued(spare)
			err := c.writeFrames(batch, time.Now().Add(writeDeadline))
			clear(batch)
			// spare was handed to the queue, so always replace it; drop burst-sized slices for gc
			spare = nil
			if cap(batch) <= 64 {
				spare = batch
			}
			if err != nil {
				return
			}

		case <-ticker.C:
			c.ws.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-c.closing:
			ticker.Stop()
			c.drainMessages()
			// closeCode is written before closing is closed, so reading it here is race free
			c.ws.WriteMessage(websocket.CloseMessage, c.closeFrame())
			return
		}
	}
}

func (c *Connection) closeFrame() []byte {
	if c.closeCode == 0 {
		return []byte{}
	}
	return websocket.FormatCloseMessage(c.closeCode, c.closeReason)
}

func (c *Connection) drainMessages() {
	c.writeFrames(c.takeQueued(nil), time.Now().Add(5*time.Second))
}

func (c *Connection) takeQueued(spare [][]byte) [][]byte {
	c.sendMu.Lock()
	batch := c.sendQueue
	c.sendQueue = spare[:0]
	c.queuedBytes = 0
	c.sendMu.Unlock()
	return batch
}

// encodes all queued messages into one buffer so a burst costs a single write syscall
func (c *Connection) writeFrames(messages [][]byte, deadline time.Time) error {
	if len(messages) == 0 {
		return nil
	}

	bp := frameBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	for _, message := range messages {
		buf = appendTextFrame(buf, message)
	}

	netConn := c.ws.NetConn()
	netConn.SetWriteDeadline(deadline)
	_, err := netConn.Write(buf)

	if cap(buf) <= 256*1024 {
		*bp = buf
		frameBufPool.Put(bp)
	}
	return err
}

// unmasked, uncompressed server frame per RFC 6455 section 5.2
func appendTextFrame(buf, payload []byte) []byte {
	n := len(payload)
	switch {
	case n < 126:
		buf = append(buf, 0x81, byte(n))
	case n <= 0xFFFF:
		buf = append(buf, 0x81, 126, byte(n>>8), byte(n))
	default:
		buf = append(buf, 0x81, 127)
		buf = binary.BigEndian.AppendUint64(buf, uint64(n))
	}
	return append(buf, payload...)
}

func (c *Connection) SendMessage(msg *protocol.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if log.GetLevel() == log.DebugLevel {
		channelName := ""
		if msg.Channel != nil {
			channelName = *msg.Channel
		}
		log.Debug("sending message", "connection", c.ID, "event", msg.Event, "channel", channelName)
	}

	return c.sendBytes(data)
}

func (c *Connection) sendBytes(data []byte) error {
	c.sendMu.Lock()
	if c.slowConsumer {
		c.sendMu.Unlock()
		return errSendBufferFull
	}
	if c.queuedBytes+len(data) > maxQueuedBytes {
		c.slowConsumer = true
		queued := c.queuedBytes
		c.sendMu.Unlock()

		log.Warn("disconnecting slow client", "connection", c.ID, "app", c.AppKey, "queued_bytes", queued)
		c.CloseWithCode(protocol.ErrorOverCapacity, "Client too slow to receive messages")
		return errSendBufferFull
	}
	c.sendQueue = append(c.sendQueue, data)
	c.queuedBytes += len(data)
	c.sendMu.Unlock()

	select {
	case c.sendReady <- struct{}{}:
	default:
	}
	return nil
}

func (c *Connection) Subscribe(channel string) {
	c.channelsMux.Lock()
	c.channels[channel] = true
	c.channelsMux.Unlock()

	if log.GetLevel() == log.DebugLevel {
		log.Debug("subscribed to channel", "connection", c.ID, "channel", channel)
	}
}

func (c *Connection) Unsubscribe(channel string) {
	c.channelsMux.Lock()
	delete(c.channels, channel)
	c.channelsMux.Unlock()

	if log.GetLevel() == log.DebugLevel {
		log.Debug("unsubscribed from channel", "connection", c.ID, "channel", channel)
	}
}

func (c *Connection) IsSubscribed(channel string) bool {
	c.channelsMux.RLock()
	defer c.channelsMux.RUnlock()
	return c.channels[channel]
}

func (c *Connection) GetChannels() []string {
	c.channelsMux.RLock()
	defer c.channelsMux.RUnlock()

	channels := make([]string, 0, len(c.channels))
	for channel := range c.channels {
		channels = append(channels, channel)
	}
	return channels
}

func (c *Connection) handleMessage(data []byte) {
	var msg protocol.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		if log.GetLevel() == log.DebugLevel {
			log.Debug("failed to parse message", "connection", c.ID, "error", err)
		}
		c.sendError("Invalid JSON", nil)
		return
	}

	switch msg.Event {
	case protocol.EventPing:
		c.handlePing()
	case protocol.EventSubscribe:
		c.handleSubscribe(&msg)
	case protocol.EventUnsubscribe:
		c.handleUnsubscribe(&msg)
	case protocol.EventSignin:
		c.handleSignin(&msg)
	default:
		c.handleClientEvent(&msg)
	}
}

func (c *Connection) handlePing() {
	pong, err := protocol.NewPong()
	if err != nil {
		return
	}
	c.SendMessage(pong)
}

func (c *Connection) handleSubscribe(msg *protocol.Message) {
	if msg.Data == "" {
		msg.Data = "{}"
	}

	subData, err := protocol.ParseSubscribeData(msg.Data)
	if err != nil {
		c.sendError("Invalid subscription data", nil)
		return
	}

	c.manager.SubscribeConnection(c, subData)
}

func (c *Connection) handleUnsubscribe(msg *protocol.Message) {
	subData, err := protocol.ParseSubscribeData(msg.Data)
	if err != nil {
		c.sendError("Invalid subscription data", nil)
		return
	}

	c.manager.UnsubscribeConnection(c, subData.Channel)
}

func (c *Connection) handleSignin(msg *protocol.Message) {
	var data struct {
		Auth     string `json:"auth"`
		UserData string `json:"user_data"`
	}
	if err := json.Unmarshal([]byte(msg.Data), &data); err != nil {
		c.sendError("Invalid signin data", nil)
		return
	}

	c.manager.SigninConnection(c, data.Auth, data.UserData)
}

func (c *Connection) handleClientEvent(msg *protocol.Message) {
	if len(msg.Event) < 7 || msg.Event[:7] != "client-" {
		return
	}

	// rate limit client events (error 4301 per Pusher protocol)
	if !c.rateLimiter.Allow() {
		code := protocol.ErrorClientEventRateLimitReached
		c.sendError("Rate limit exceeded for client events", &code)
		return
	}

	if c.manager.appsManager != nil {
		app, exists := c.manager.appsManager.GetApp(c.AppKey)
		if exists && !app.IsClientEventsEnabled() {
			code := protocol.ErrorConnectionIsUnauthorized
			c.sendError("Client events are not enabled for this app", &code)
			return
		}
	}

	if msg.Channel == nil {
		code := protocol.ErrorConnectionIsUnauthorized
		c.sendError("Client events require a channel", &code)
		return
	}

	channelName := *msg.Channel

	// channel must be private or presence (encrypted channels do NOT support client events per spec)
	if !protocol.IsPrivateChannel(channelName) && !protocol.IsPresenceChannel(channelName) {
		code := protocol.ErrorConnectionIsUnauthorized
		c.sendError("Client events are only supported on private and presence channels", &code)
		return
	}

	// encrypted channels explicitly do not support client events
	if protocol.IsEncryptedChannel(channelName) {
		code := protocol.ErrorConnectionIsUnauthorized
		c.sendError("Client events are not supported on encrypted channels", &code)
		return
	}

	if !c.IsSubscribed(channelName) {
		return
	}

	if protocol.IsPresenceChannel(channelName) {
		if member, ok := c.manager.presenceManager.GetMember(channelKey(c.AppKey, channelName), c.ID); ok {
			msg.UserID = member.UserID
		}
	}

	c.manager.BroadcastToChannel(c.AppKey, channelName, msg, c.ID)
}

func (c *Connection) sendError(message string, code *int) {
	errMsg, err := protocol.NewError(message, code)
	if err != nil {
		return
	}
	c.SendMessage(errMsg)
}

func (c *Connection) closeWebSocket() {
	c.closeOnce.Do(func() {
		c.ws.Close()
	})
}

func (c *Connection) Close() {
	c.CloseWithCode(0, "")
}

func (c *Connection) CloseWithCode(code int, reason string) {
	c.closeMux.Lock()
	defer c.closeMux.Unlock()

	if c.isClosed {
		return
	}
	c.isClosed = true
	c.closeCode = code
	c.closeReason = reason

	close(c.closing)
}
