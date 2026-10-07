import { afterEach, describe, expect, it } from "vitest";
import { APP_NO_CLIENT, APP_RATELIMITED } from "../src/constants.js";
import {
  disconnectAll,
  expectNoEvent,
  makeClient,
  subscribeAndWait,
  waitForConnection,
  waitForConnectionError,
  waitForEvent,
  type PusherClient,
} from "../src/clients.js";

describe("client events", () => {
  let clients: PusherClient[] = [];

  afterEach(async () => {
    await disconnectAll(...clients);
    clients = [];
  });

  it("broadcasts a client event to other subscribers but not the sender", async () => {
    const sender = makeClient();
    const receiver = makeClient();
    clients.push(sender, receiver);
    await Promise.all([waitForConnection(sender), waitForConnection(receiver)]);

    const senderCh = await subscribeAndWait(sender, "private-chat");
    const receiverCh = await subscribeAndWait(receiver, "private-chat");

    const payload = { text: "hi there" };
    const gotByReceiver = waitForEvent<typeof payload>(receiverCh, "client-message");
    // Sender must never see its own client event echoed back.
    const senderEcho = expectNoEvent(senderCh, "client-message", 1500);

    const ok = senderCh.trigger("client-message", payload);
    expect(ok).toBe(true);

    expect(await gotByReceiver).toEqual(payload);
    await senderEcho;
  });

  it("rejects client events when the app has them disabled", async () => {
    const client = makeClient({ app: APP_NO_CLIENT });
    clients.push(client);
    await waitForConnection(client);
    const channel = await subscribeAndWait(client, "private-disabled");

    const errored = waitForConnectionError(client);
    channel.trigger("client-nope", { a: 1 });

    const { code } = await errored;
    // 4009: connection unauthorized for client events.
    expect(code).toBe(4009);
  });

  it("rate limits client events with code 4301", async () => {
    const client = makeClient({ app: APP_RATELIMITED });
    clients.push(client);
    await waitForConnection(client);
    const channel = await subscribeAndWait(client, "private-rl");

    const errored = waitForConnectionError(client);
    // Burst is 2; flood past it to trip the limiter.
    for (let i = 0; i < 12; i++) {
      channel.trigger("client-spam", { i });
    }

    const { code } = await errored;
    expect(code).toBe(4301);
  });
});
