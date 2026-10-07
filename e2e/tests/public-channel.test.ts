import { afterEach, describe, expect, it } from "vitest";
import {
  disconnectAll,
  makeClient,
  makeServerClient,
  subscribeAndWait,
  waitForConnection,
  waitForEvent,
  type PusherClient,
} from "../src/clients.js";

describe("public channels", () => {
  const server = makeServerClient();
  let clients: PusherClient[] = [];

  afterEach(async () => {
    await disconnectAll(...clients);
    clients = [];
  });

  it("subscribes and receives an event triggered via the REST API", async () => {
    const client = makeClient();
    clients.push(client);
    await waitForConnection(client);

    const channel = await subscribeAndWait(client, "public-basic");
    const payload = { hello: "world", n: 42, nested: { ok: true } };

    const received = waitForEvent<typeof payload>(channel, "my-event");
    await server.trigger("public-basic", "my-event", payload);

    // The #1 fix serializes data as a JSON string per protocol; pusher-js
    // parses it back, so the client must see the exact original object.
    expect(await received).toEqual(payload);
  });

  it("delivers a single trigger to multiple channels", async () => {
    const client = makeClient();
    clients.push(client);
    await waitForConnection(client);

    const chA = await subscribeAndWait(client, "public-multi-a");
    const chB = await subscribeAndWait(client, "public-multi-b");

    const gotA = waitForEvent(chA, "fan-out");
    const gotB = waitForEvent(chB, "fan-out");
    await server.trigger(["public-multi-a", "public-multi-b"], "fan-out", { v: 1 });

    expect(await gotA).toEqual({ v: 1 });
    expect(await gotB).toEqual({ v: 1 });
  });

  it("only delivers events to subscribers of the target channel", async () => {
    const subscriber = makeClient();
    const bystander = makeClient();
    clients.push(subscriber, bystander);
    await Promise.all([waitForConnection(subscriber), waitForConnection(bystander)]);

    const target = await subscribeAndWait(subscriber, "public-target");
    const other = await subscribeAndWait(bystander, "public-other");

    let leaked = false;
    other.bind("scoped", () => {
      leaked = true;
    });

    const received = waitForEvent(target, "scoped");
    await server.trigger("public-target", "scoped", { ok: true });
    expect(await received).toEqual({ ok: true });
    expect(leaked).toBe(false);
  });
});
