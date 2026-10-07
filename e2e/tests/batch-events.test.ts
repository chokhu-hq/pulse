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

describe("batch events (REST)", () => {
  const server = makeServerClient();
  let clients: PusherClient[] = [];

  afterEach(async () => {
    await disconnectAll(...clients);
    clients = [];
  });

  it("delivers each event in a batch to its channel", async () => {
    const client = makeClient();
    clients.push(client);
    await waitForConnection(client);

    const chA = await subscribeAndWait(client, "batch-a");
    const chB = await subscribeAndWait(client, "batch-b");

    const gotA = waitForEvent(chA, "evt-a");
    const gotB = waitForEvent(chB, "evt-b");

    await server.triggerBatch([
      { channel: "batch-a", name: "evt-a", data: { from: "a" } },
      { channel: "batch-b", name: "evt-b", data: { from: "b" } },
    ]);

    expect(await gotA).toEqual({ from: "a" });
    expect(await gotB).toEqual({ from: "b" });
  });
});
