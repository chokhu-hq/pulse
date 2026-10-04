package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aelpxy/pulse/apps"
	"github.com/aelpxy/pulse/auth"
	"github.com/aelpxy/pulse/channel"
	"github.com/aelpxy/pulse/connection"
	"github.com/aelpxy/pulse/protocol"
	"github.com/charmbracelet/log"
	"github.com/gorilla/websocket"
)

type Server struct {
	appsManager    *apps.Manager
	channelManager *channel.Manager
	connectionMgr  *connection.Manager
	authServices   map[string]*auth.Service
	authMux        sync.RWMutex
	upgrader       websocket.Upgrader
	appPathRegex   *regexp.Regexp
}

type Config struct {
	AppsConfigFile string
	MaxConnections int
}

func New(config Config) (*Server, *apps.ServerConfig, error) {
	if config.MaxConnections == 0 {
		config.MaxConnections = 100000
	}

	appsMgr := apps.NewManager()
	serverConfig, err := appsMgr.LoadFromFile(config.AppsConfigFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load apps config: %w", err)
	}

	log.Info("loaded apps", "count", appsMgr.GetAppCount(), "file", config.AppsConfigFile)

	channelMgr := channel.NewManager()

	connMgr := connection.NewManager(channelMgr, nil, config.MaxConnections)

	authServices := make(map[string]*auth.Service)
	for _, app := range appsMgr.GetAllApps() {
		authServices[app.Key] = auth.NewService(app.Key, app.Secret)
		log.Info("app registered", "name", app.Name, "key", app.Key, "max_connections", app.MaxConnections)
	}

	connMgr.SetAuthServices(authServices)
	connMgr.SetAppsManager(appsMgr)

	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	return &Server{
		appsManager:    appsMgr,
		channelManager: channelMgr,
		connectionMgr:  connMgr,
		authServices:   authServices,
		upgrader:       upgrader,
		appPathRegex:   regexp.MustCompile(`^/app/([^/]+)$`),
	}, serverConfig, nil
}

func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic in websocket handler", "panic", r)
		}
	}()

	matches := s.appPathRegex.FindStringSubmatch(r.URL.Path)
	if len(matches) != 2 {
		http.Error(w, "Invalid WebSocket path", http.StatusBadRequest)
		return
	}

	appKey := matches[1]

	app, exists := s.appsManager.GetApp(appKey)
	if !exists {
		log.Warn("unknown app key", "key", appKey)
		http.Error(w, "Unknown app", http.StatusNotFound)
		return
	}

	if !app.Enabled {
		log.Warn("disabled app", "key", appKey)
		http.Error(w, "App disabled", http.StatusForbidden)
		return
	}

	origin := r.Header.Get("Origin")
	allowedOrigins := app.GetAllowedOrigins()
	originAllowed := false
	for _, allowed := range allowedOrigins {
		if origin == allowed || allowed == "*" {
			originAllowed = true
			break
		}
	}
	if !originAllowed && origin != "" {
		log.Warn("origin not allowed", "app", appKey, "origin", origin)
		http.Error(w, "Origin not allowed", http.StatusForbidden)
		return
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("failed to upgrade connection", "error", err)
		return
	}

	ws.SetReadLimit(app.GetMaxMessageSize())

	conn, err := s.connectionMgr.RegisterWithApp(ws, appKey)
	if err != nil {
		if errors.Is(err, connection.ErrMaxConnectionsReached) || errors.Is(err, connection.ErrAppMaxConnections) {
			log.Warn("max connections reached", "app", appKey)
			ws.WriteMessage(1, []byte(`{"event":"pusher:error","data":{"message":"Over capacity","code":4100}}`))
		} else {
			log.Error("failed to register connection", "error", err)
		}
		ws.Close()
		return
	}

	log.Debug("new connection", "id", conn.ID, "app", appKey)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in write pump", "connection", conn.ID, "panic", r)
			}
		}()
		conn.WritePump()
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in read pump", "connection", conn.ID, "panic", r)
				s.connectionMgr.Unregister(conn)
			}
		}()
		conn.ReadPump()
	}()
}

func (s *Server) HandleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	appID := parts[1]

	targetApp, exists := s.appsManager.GetAppByID(appID)
	if !exists {
		http.Error(w, "App not found", http.StatusNotFound)
		return
	}

	// limit request body size (default 10KB, or app's max message size)
	maxBodySize := max(targetApp.GetMaxMessageSize(), 10240)
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	s.authMux.RLock()
	authSvc, exists := s.authServices[targetApp.Key]
	s.authMux.RUnlock()

	if !exists {
		http.Error(w, "Authentication service not found", http.StatusInternalServerError)
		return
	}

	if err := authSvc.ValidateHTTPRequest(r.Method, r.URL.Path, r.URL.Query(), body); err != nil {
		log.Warn("authentication failed", "error", err, "app", targetApp.Key, "path", r.URL.Path)
		http.Error(w, fmt.Sprintf("Authentication failed: %v", err), http.StatusUnauthorized)
		return
	}

	type TriggerRequest struct {
		Name     string   `json:"name"`
		Channel  string   `json:"channel"`
		Channels []string `json:"channels"`
		Data     string   `json:"data"`
		SocketID string   `json:"socket_id"`
		Info     string   `json:"info"`
	}

	var trigger TriggerRequest
	if err := json.Unmarshal(body, &trigger); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	channels := trigger.Channels
	if trigger.Channel != "" {
		channels = append(channels, trigger.Channel)
	}

	for _, ch := range channels {
		s.connectionMgr.PublishToChannel(targetApp.Key, ch, trigger.Name, trigger.Data, trigger.SocketID)
	}

	w.Header().Set("Content-Type", "application/json")
	if trigger.Info == "" {
		fmt.Fprintf(w, `{}`)
		return
	}

	infos := make(map[string]channelInfo, len(channels))
	for _, ch := range channels {
		infos[ch] = s.getChannelInfo(targetApp.Key, ch, trigger.Info)
	}
	json.NewEncoder(w).Encode(map[string]any{"channels": infos})
}

func (s *Server) HandleBatchEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	appID := parts[1]

	targetApp, exists := s.appsManager.GetAppByID(appID)
	if !exists {
		http.Error(w, "App not found", http.StatusNotFound)
		return
	}

	// limit request body size for batch (10x single message size)
	// min 100KB for batch
	maxBodySize := max(targetApp.GetMaxMessageSize()*10,
		102400)
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	s.authMux.RLock()
	authSvc, exists := s.authServices[targetApp.Key]
	s.authMux.RUnlock()

	if !exists {
		http.Error(w, "Authentication service not found", http.StatusInternalServerError)
		return
	}

	if err := authSvc.ValidateHTTPRequest(r.Method, r.URL.Path, r.URL.Query(), body); err != nil {
		log.Warn("authentication failed", "error", err, "app", targetApp.Key, "path", r.URL.Path)
		http.Error(w, fmt.Sprintf("Authentication failed: %v", err), http.StatusUnauthorized)
		return
	}

	type BatchEvent struct {
		Name     string `json:"name"`
		Channel  string `json:"channel"`
		Data     string `json:"data"`
		Info     string `json:"info,omitempty"`
		SocketID string `json:"socket_id"`
	}

	type BatchRequest struct {
		Batch []BatchEvent `json:"batch"`
	}

	var batchReq BatchRequest
	if err := json.Unmarshal(body, &batchReq); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	maxBatch := targetApp.GetMaxBatchEvents()
	if len(batchReq.Batch) > maxBatch {
		http.Error(w, fmt.Sprintf("Batch size exceeds maximum of %d events", maxBatch), http.StatusRequestEntityTooLarge)
		return
	}

	responses := make([]channelInfo, len(batchReq.Batch))

	for i, event := range batchReq.Batch {
		s.connectionMgr.PublishToChannel(targetApp.Key, event.Channel, event.Name, event.Data, event.SocketID)
		responses[i] = s.getChannelInfo(targetApp.Key, event.Channel, event.Info)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"batch": responses,
	})
}

type channelInfo struct {
	SubscriptionCount *int `json:"subscription_count,omitempty"`
	UserCount         *int `json:"user_count,omitempty"`
}

func (s *Server) getChannelInfo(appKey, channelName, info string) channelInfo {
	var ci channelInfo
	for attr := range strings.SplitSeq(info, ",") {
		switch strings.TrimSpace(attr) {
		case "subscription_count":
			count := s.connectionMgr.GetSubscriberCount(appKey, channelName)
			ci.SubscriptionCount = &count
		case "user_count":
			if protocol.IsPresenceChannel(channelName) {
				count := s.connectionMgr.GetUserCount(appKey, channelName)
				ci.UserCount = &count
			}
		}
	}
	return ci
}

func (s *Server) HandleChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	targetApp, exists := s.appsManager.GetAppByID(parts[1])
	if !exists {
		http.Error(w, "App not found", http.StatusNotFound)
		return
	}

	s.authMux.RLock()
	authSvc, exists := s.authServices[targetApp.Key]
	s.authMux.RUnlock()

	if !exists {
		http.Error(w, "Authentication service not found", http.StatusInternalServerError)
		return
	}

	if err := authSvc.ValidateHTTPRequest(r.Method, r.URL.Path, r.URL.Query(), nil); err != nil {
		log.Warn("authentication failed", "error", err, "app", targetApp.Key, "path", r.URL.Path)
		http.Error(w, fmt.Sprintf("Authentication failed: %v", err), http.StatusUnauthorized)
		return
	}

	info := r.URL.Query().Get("info")
	wantsUserCount := slices.Contains(strings.Split(info, ","), "user_count")

	var resp any
	switch {
	case len(parts) == 3:
		prefix := r.URL.Query().Get("filter_by_prefix")
		if wantsUserCount && !protocol.IsPresenceChannel(prefix) {
			http.Error(w, "user_count requires filter_by_prefix=presence-", http.StatusBadRequest)
			return
		}
		channels := make(map[string]channelInfo)
		for _, name := range s.connectionMgr.GetChannelNames(targetApp.Key, prefix) {
			channels[name] = s.getChannelInfo(targetApp.Key, name, info)
		}
		resp = map[string]any{"channels": channels}

	case len(parts) == 4:
		channelName := parts[3]
		if wantsUserCount && !protocol.IsPresenceChannel(channelName) {
			http.Error(w, "user_count is only available for presence channels", http.StatusBadRequest)
			return
		}
		resp = struct {
			Occupied bool `json:"occupied"`
			channelInfo
		}{
			Occupied:    s.connectionMgr.GetSubscriberCount(targetApp.Key, channelName) > 0,
			channelInfo: s.getChannelInfo(targetApp.Key, channelName, info),
		}

	case len(parts) == 5 && parts[4] == "users":
		channelName := parts[3]
		if !protocol.IsPresenceChannel(channelName) {
			http.Error(w, "Users are only available for presence channels", http.StatusBadRequest)
			return
		}
		type user struct {
			ID string `json:"id"`
		}
		users := make([]user, 0)
		for _, id := range s.connectionMgr.GetUserIDs(targetApp.Key, channelName) {
			users = append(users, user{ID: id})
		}
		resp = map[string]any{"users": users}

	default:
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) HandleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.connectionMgr.GetStats()
	stats["apps_loaded"] = s.appsManager.GetAppCount()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) HandleApps(w http.ResponseWriter, r *http.Request) {
	allApps := s.appsManager.GetAllApps()

	type AppInfo struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}

	appsInfo := make([]AppInfo, 0, len(allApps))
	for _, app := range allApps {
		appsInfo = append(appsInfo, AppInfo{
			ID:  app.ID,
			Key: app.Key,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"apps":  appsInfo,
		"count": len(appsInfo),
	})
}

func (s *Server) Shutdown(timeout time.Duration) error {
	return s.connectionMgr.Shutdown(timeout)
}

func (s *Server) GetAppsManager() *apps.Manager {
	return s.appsManager
}

func (s *Server) ReloadApps(configFile string) error {
	newAppsMgr := apps.NewManager()
	_, err := newAppsMgr.LoadFromFile(configFile)
	if err != nil {
		return fmt.Errorf("failed to reload apps: %w", err)
	}

	// Update auth services
	s.authMux.Lock()
	s.authServices = make(map[string]*auth.Service)
	for _, app := range newAppsMgr.GetAllApps() {
		s.authServices[app.Key] = auth.NewService(app.Key, app.Secret)
	}
	authServicesCopy := make(map[string]*auth.Service)
	for k, v := range s.authServices {
		authServicesCopy[k] = v
	}
	s.authMux.Unlock()

	// Update connection manager's auth services
	s.connectionMgr.SetAuthServices(authServicesCopy)

	// Update apps manager (thread-safe as it's only read after startup)
	s.appsManager = newAppsMgr

	log.Info("reloaded apps", "count", newAppsMgr.GetAppCount())
	return nil
}
