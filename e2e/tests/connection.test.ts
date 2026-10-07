import { afterEach, describe, expect, it } from "vitest";
import {
  disconnectAll,
  makeClient,
  waitForConnection,
  type PusherClient,
} from "../src/clients.js";

describe("connection handshake", () => {
  let clients: PusherClient[] = [];
  afterEach(async () => {
    await disconnectAll(...clients);
    clients = [];
  });

  it("establishes a connection and receives a socket_id", async () => {
    const client = makeClient();
    clients.push(client);

    const socketId = await waitForConnection(client);

    // Pusher socket ids are two decimal integers separated by a dot; the
    // official SDKs reject any other format (this guards the socket-id fix).
    expect(socketId).toBeTruthy();
    expect(socketId).toMatch(/^\d+\.\d+$/);
    expect(client.connection.state).toBe("connected");
  });
});
