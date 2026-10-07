import { afterEach, describe, expect, it } from "vitest";
import {
  asPresence,
  disconnectAll,
  makeClient,
  subscribeAndWait,
  waitForConnection,
  waitForEvent,
  type PusherClient,
} from "../src/clients.js";

interface Member {
  id: string;
  info: Record<string, unknown> | null;
}

describe("presence channels", () => {
  let clients: PusherClient[] = [];

  afterEach(async () => {
    await disconnectAll(...clients);
    clients = [];
  });

  it("reports the current member on subscription and tracks join/leave", async () => {
    const alice = makeClient({ presence: { user_id: "alice", user_info: { name: "Alice" } } });
    clients.push(alice);
    await waitForConnection(alice);

    const aliceCh = asPresence(await subscribeAndWait(alice, "presence-lobby"));
    expect(aliceCh.members.count).toBe(1);
    expect(aliceCh.members.me?.id).toBe("alice");
    expect(aliceCh.members.me?.info).toEqual({ name: "Alice" });

    // Alice should be told when Bob joins.
    const memberAdded = waitForEvent<Member>(aliceCh, "pusher:member_added");

    const bob = makeClient({ presence: { user_id: "bob", user_info: { name: "Bob" } } });
    clients.push(bob);
    await waitForConnection(bob);
    const bobCh = asPresence(await subscribeAndWait(bob, "presence-lobby"));

    // Bob's own subscription snapshot must already include Alice.
    expect(bobCh.members.count).toBe(2);
    expect(bobCh.members.me?.id).toBe("bob");
    expect(bobCh.members.get("alice")?.info).toEqual({ name: "Alice" });

    const added = await memberAdded;
    expect(added.id).toBe("bob");
    expect(added.info).toEqual({ name: "Bob" });
    expect(aliceCh.members.count).toBe(2);

    // And when Bob leaves.
    const memberRemoved = waitForEvent<Member>(aliceCh, "pusher:member_removed");
    bob.disconnect();
    const removed = await memberRemoved;
    expect(removed.id).toBe("bob");
    expect(aliceCh.members.count).toBe(1);
  });
});
