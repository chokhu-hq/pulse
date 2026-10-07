import Pusher from "pusher";
import PusherClientImport from "pusher-js";
import type { Channel, PresenceChannel } from "pusher-js";
import { HOST, PORT, APP, type AppCreds } from "./constants.js";

const PusherClientCtor = ((PusherClientImport as unknown as { Pusher?: typeof PusherClientImport })
  .Pusher ?? PusherClientImport) as typeof PusherClientImport;

export type PusherClient = InstanceType<typeof PusherClientImport>;

(PusherClientCtor as unknown as { logToConsole: boolean }).logToConsole =
  process.env.PUSHER_JS_DEBUG === "1";

export interface PresenceMember {
  user_id: string;
  user_info?: Record<string, unknown>;
}

export interface ClientOptions {
  app?: AppCreds;
  presence?: PresenceMember;
  badAuth?: boolean;
}

export function makeServerClient(app: AppCreds = APP): Pusher {
  return new Pusher({
    appId: app.appId,
    key: app.key,
    secret: app.secret,
    host: HOST,
    port: String(PORT),
    useTLS: false,
  });
}

const serverClients = new Map<string, Pusher>();

function serverClientFor(app: AppCreds): Pusher {
  let c = serverClients.get(app.key);
  if (!c) {
    c = makeServerClient(app);
    serverClients.set(app.key, c);
  }
  return c;
}

export function makeClient(opts: ClientOptions = {}): PusherClient {
  const app = opts.app ?? APP;
  const signer = serverClientFor(app);

  return new PusherClientCtor(app.key, {
    wsHost: HOST,
    wsPort: PORT,
    forceTLS: false,
    enableStats: false,
    enabledTransports: ["ws"],
    cluster: "e2e",
    channelAuthorization: {
      transport: "ajax",
      endpoint: "<unused-custom-handler>",
      customHandler: (params, callback) => {
        try {
          if (opts.badAuth) {
            callback(null, { auth: `${app.key}:deadbeef`, channel_data: undefined });
            return;
          }
          const isPresence = params.channelName.startsWith("presence-");
          const authData = isPresence
            ? signer.authorizeChannel(
              params.socketId,
              params.channelName,
              opts.presence ?? { user_id: params.socketId },
            )
            : signer.authorizeChannel(params.socketId, params.channelName);
          callback(null, authData as never);
        } catch (err) {
          callback(err as Error, null);
        }
      },
    },
  });
}

const DEFAULT_TIMEOUT = 8_000;

export function waitForConnection(
  client: PusherClient,
  timeoutMs = DEFAULT_TIMEOUT,
): Promise<string> {
  return new Promise((resolve, reject) => {
    if (client.connection.state === "connected") {
      resolve(client.connection.socket_id);
      return;
    }
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out waiting for connection (state=${client.connection.state})`));
    }, timeoutMs);
    const onConnected = () => {
      cleanup();
      resolve(client.connection.socket_id);
    };
    const onError = (err: unknown) => {
      cleanup();
      reject(new Error(`connection error: ${JSON.stringify(err)}`));
    };
    const cleanup = () => {
      clearTimeout(timer);
      client.connection.unbind("connected", onConnected);
      client.connection.unbind("error", onError);
    };
    client.connection.bind("connected", onConnected);
    client.connection.bind("error", onError);
  });
}

export function subscribeAndWait(
  client: PusherClient,
  channelName: string,
  timeoutMs = DEFAULT_TIMEOUT,
): Promise<Channel> {
  const channel = client.subscribe(channelName);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out subscribing to ${channelName}`));
    }, timeoutMs);
    const onOk = () => {
      cleanup();
      resolve(channel);
    };
    const onErr = (err: unknown) => {
      cleanup();
      reject(new Error(`subscription_error on ${channelName}: ${JSON.stringify(err)}`));
    };
    const cleanup = () => {
      clearTimeout(timer);
      channel.unbind("pusher:subscription_succeeded", onOk);
      channel.unbind("pusher:subscription_error", onErr);
    };
    channel.bind("pusher:subscription_succeeded", onOk);
    channel.bind("pusher:subscription_error", onErr);
  });
}

export function waitForEvent<T = unknown>(
  channel: Channel,
  event: string,
  timeoutMs = DEFAULT_TIMEOUT,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      channel.unbind(event, handler);
      reject(new Error(`timed out waiting for event "${event}" on ${channel.name}`));
    }, timeoutMs);
    const handler = (data: T) => {
      clearTimeout(timer);
      channel.unbind(event, handler);
      resolve(data);
    };
    channel.bind(event, handler);
  });
}

export function expectNoEvent(
  channel: Channel,
  event: string,
  windowMs = 1_000,
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const handler = (data: unknown) => {
      channel.unbind(event, handler);
      reject(new Error(`unexpected event "${event}" fired: ${JSON.stringify(data)}`));
    };
    channel.bind(event, handler);
    setTimeout(() => {
      channel.unbind(event, handler);
      resolve(true);
    }, windowMs);
  });
}

export interface ConnectionError {
  code?: number;
  raw: unknown;
}

export function waitForConnectionError(
  client: PusherClient,
  timeoutMs = DEFAULT_TIMEOUT,
): Promise<ConnectionError> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      client.connection.unbind("error", handler);
      reject(new Error("timed out waiting for a connection error"));
    }, timeoutMs);
    const handler = (err: unknown) => {
      clearTimeout(timer);
      client.connection.unbind("error", handler);
      const e = err as { code?: number; data?: { code?: number }; error?: { data?: { code?: number } } };
      const code = e?.data?.code ?? e?.error?.data?.code ?? e?.code;
      resolve({ code, raw: err });
    };
    client.connection.bind("error", handler);
  });
}

export function asPresence(channel: Channel): PresenceChannel {
  return channel as PresenceChannel;
}

export async function disconnectAll(...clients: PusherClient[]): Promise<void> {
  for (const c of clients) {
    try {
      c.disconnect();
    } catch {
      // ignore
    }
  }
}
