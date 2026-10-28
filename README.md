# Pulse

A Pusher Protocol v7 compatible server for Linux, written in Rust. Pulse implements the official Pusher Channels protocol, allowing you to use any Pusher SDK with your own self-hosted server.

## Benchmarks

<!-- bench:start -->

> Measured automatically on a shared GitHub Actions runner ([run](https://github.com/chokhu-hq/pulse/actions/runs/39974255200), commit `179ffba`). Expect lower numbers and some run-to-run variance compared to dedicated hardware.

linux/amd64, 4 CPUs, payload 200 bytes, server and load generator on the same host, measured 2026-10-23

| Scenario | Subscribers | Publishers | Publishes/s | Deliveries/s | Dropped | Server CPU / delivery | RSS / conn |
|---|---:|---:|---:|---:|---:|---:|---:|
| publish throughput | 1 | 256 | 64.5k | 64.5k | 0 | 33.70 µs | - |
| fan-out 100 subscribers | 100 | 32 | 15.1k | 1.50M | 0 | 1.43 µs | - |
| fan-out 1000 subscribers | 1000 | 16 | 1.3k | 1.31M | 0 | 1.70 µs | 8.7 KB |
| fan-out 5000 subscribers | 5000 | 16 | 532 | 2.56M | 0 | 0.98 µs | 8.0 KB |

<!-- bench:end -->

## Installation

### Build from Source

Requirements: Linux and Rust 1.99 or newer. Prebuilt binaries need glibc 2.35 or newer.

```bash
# Clone the repository
git clone https://github.com/aelpxy/pulse.git
cd pulse

# Build binary
cargo build --release

# Run the server
./target/release/pulse --config config.toml
```

### Command Line Flags

| Flag | Environment | Description |
| ---------- | ------ | ------------- |
| `--config <path>` | `PULSE_CONFIG` | Path to the config file (default: `config.toml`) |
| `--port <port>` | `PULSE_PORT` | Port to listen on, overriding the config file |
| `--max-connections <n>` | | Maximum concurrent connections across all apps (default: 100000) |
| `--debug` | | Enable debug logging, overriding the config file |

Pulse uses one worker thread per CPU by default. Set `TOKIO_WORKER_THREADS` to use fewer, for example when sharing the host with other busy processes; fewer threads also batch more messages per write, which lowers CPU per delivery.

## Usage Examples

### Server Example (TypeScript)

```typescript
import Pusher from "pusher";

const pusher = new Pusher({
  appId: "your-pulse-dev",
  key: "your-pulse-dev-key",
  secret: "your-pulse-dev-secret",
  useTLS: false,
  host: "localhost:8080",
});

setInterval(async () => {
  try {
    const timestamp = new Date().toISOString();
    const message = {
      text: "Hello from Pulse!",
      timestamp,
      count: Math.floor(Math.random() * 100),
    };

    await pusher.trigger("my-channel", "my-event", message);
  } catch (error) {}
}, 5000);
```

### Client Example (TypeScript)

```typescript
import Pusher from "pusher-js";

const pusher = new Pusher("your-pulse-dev-key", {
  wsHost: "localhost",
  wsPort: 8080,
  forceTLS: false,
  enableStats: false,
  enabledTransports: ["ws", "wss"],
});

const channel = pusher.subscribe("my-channel");

channel.bind("my-event", (data: any) => {
  console.log(data);
  console.log("");
});
```

## Configuration

An example config in TOML (name this as `config.toml` or pass its path via `--config path`). Each app is an `[[apps]]` table. Unknown fields are rejected so typos fail at startup.

```toml
[server]
debug = true
port = 8080
hostname = ""
region = ""

[[apps]]
id = "your-pulse-dev"
key = "your-pulse-dev-key"
secret = "your-pulse-dev-secret"
name = "pulse app"
enabled = true
max_connections = 10000
max_channels_per_connection = 50
allowed_origins = ["*"]
max_message_size = 8192
max_batch_events = 100
enable_client_events = true
max_event_rate = 10
max_event_burst = 20
```

### Configuration Properties

#### Server Properties

| Property | Type | Description |
| ---------- | ------ | ------------- |
| `debug` | boolean | Enable debug logging for troubleshooting |
| `port` | integer | Port number the server listens on (default: 8080) |
| `max_http_connections` | integer | Maximum concurrent HTTP connections, not counting upgraded WebSocket connections. Extra connections are closed (default: 200000) |
| `hostname` | string | Accepted for compatibility; the server always listens on all interfaces |
| `region` | string | Accepted for compatibility; currently unused |

#### App Properties

| Property | Type | Description |
| ---------- | ------ | ------------- |
| `id` | string | Unique identifier for the application |
| `key` | string | Public key used by clients to connect |
| `secret` | string | Secret key used for authentication and signing |
| `name` | string | Human-readable name for the application |
| `enabled` | boolean | Whether the app is active and accepting connections |
| `max_connections` | number | Maximum number of concurrent WebSocket connections |
| `max_channels_per_connection` | number | Maximum channels a single connection can subscribe to |
| `allowed_origins` | array | CORS allowed origins (`["*"]` for all origins) |
| `max_message_size` | number | Maximum message size in bytes |
| `max_batch_events` | number | Maximum number of events in a batch trigger |
| `enable_client_events` | boolean | Allow clients to trigger events prefixed with `client-` |
| `max_event_rate` | number | Maximum client events per second (default: 10) |
| `max_event_burst` | number | Maximum burst capacity for client events (default: 20) |

## Securing operational endpoints

`/stats`, `/apps` and `/metrics` are unauthenticated and `/apps` lists app keys. Pulse leaves access control to the reverse proxy, so restrict these paths there and expose only `/app/*` and `/apps/*` publicly.

## License

[MIT](./LICENSE)
