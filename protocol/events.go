package protocol

const (
	// system events
	EventConnectionEstablished = "pusher:connection_established"
	EventError                 = "pusher:error"
	EventPing                  = "pusher:ping"
	EventPong                  = "pusher:pong"
	EventSubscribe             = "pusher:subscribe"
	EventUnsubscribe           = "pusher:unsubscribe"
	EventSignin                = "pusher:signin"
	EventSigninSuccess         = "pusher:signin_success"

	// internal events
	EventSubscriptionSucceeded = "pusher_internal:subscription_succeeded"
	EventMemberAdded           = "pusher_internal:member_added"
	EventMemberRemoved         = "pusher_internal:member_removed"
)

// channel name prefixes
const (
	PrivateChannelPrefix          = "private-"
	PresenceChannelPrefix         = "presence-"
	PrivateEncryptedChannelPrefix = "private-encrypted-"
	ServerToUserChannelPrefix     = "#server-to-user-"
)

// socket close codes
const (
	// 4000-4099: do not reconnect unchanged
	CloseNormalClosure        = 4000
	CloseApplicationError     = 4001
	CloseOverCapacity         = 4004
	ClosePathNotFound         = 4005
	CloseInvalidVersionString = 4006
	CloseUnsupportedProtocol  = 4007
	CloseNoProtocolVersion    = 4008

	// 4100-4199: reconnect after exponential backoff
	CloseAlreadyAuthenticated       = 4100
	CloseInvalidSignature           = 4101
	CloseInvalidOrigin              = 4009
	CloseClientOverRateLimit        = 4301
	ClosePongReplyNotReceived       = 4201
	CloseClosedAfterInactivity      = 4202
	CloseClientEventRateLimited     = 4302
	CloseServerError                = 4500
	CloseApplicationNotFound        = 4001
	CloseApplicationDisabled        = 4003
	CloseApplicationOverQuota       = 4004
	CloseApplicationOverConnections = 4100

	// 4200-4299: reconnect immediately
	CloseTLSInvalid = 4200
)

// error codes
const (
	ErrorApplicationOnlyAcceptsSSL          = 4000
	ErrorApplicationDoesNotExist            = 4001
	ErrorApplicationDisabled                = 4003
	ErrorApplicationOverConnectionQuota     = 4004
	ErrorPathNotFound                       = 4005
	ErrorInvalidVersionStringFormat         = 4006
	ErrorUnsupportedProtocolVersion         = 4007
	ErrorNoProtocolVersionSupplied          = 4008
	ErrorConnectionIsUnauthorized           = 4009
	ErrorOverCapacity                       = 4100
	ErrorGenericReconnect                   = 4200
	ErrorPongReplyNotReceived               = 4201
	ErrorClosedAfterInactivityTimeout       = 4202
	ErrorClientEventRateLimitReached        = 4301
	ErrorUpstreamWebsocketOrHTTPServerError = 4500
	ErrorApplicationNotFoundInConfiguration = 4001
	ErrorApplicationDisabledInConfiguration = 4003
	ErrorApplicationOverConnectionLimit     = 4004
)

func IsEncryptedChannel(channel string) bool {
	return len(channel) >= len(PrivateEncryptedChannelPrefix) &&
		channel[:len(PrivateEncryptedChannelPrefix)] == PrivateEncryptedChannelPrefix
}

func IsPrivateChannel(channel string) bool {
	// encrypted channels start with "private-encrypted-" so check that first
	if IsEncryptedChannel(channel) {
		return false
	}

	return len(channel) >= len(PrivateChannelPrefix) &&
		channel[:len(PrivateChannelPrefix)] == PrivateChannelPrefix
}

func IsPresenceChannel(channel string) bool {
	return len(channel) >= len(PresenceChannelPrefix) &&
		channel[:len(PresenceChannelPrefix)] == PresenceChannelPrefix
}

func IsPublicChannel(channel string) bool {
	return !IsPrivateChannel(channel) && !IsPresenceChannel(channel) && !IsEncryptedChannel(channel)
}
