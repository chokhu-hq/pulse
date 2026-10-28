import { EventEmitter } from "node:events";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  existsSync: vi.fn(),
  copyFileSync: vi.fn(),
  spawn: vi.fn(),
  spawnSync: vi.fn(),
}));

vi.mock("node:fs", () => ({
  existsSync: mocks.existsSync,
  copyFileSync: mocks.copyFileSync,
}));
vi.mock("node:child_process", () => ({
  spawn: mocks.spawn,
  spawnSync: mocks.spawnSync,
}));

let proc: EventEmitter & { exitCode: number | null; kill: ReturnType<typeof vi.fn> };

beforeEach(() => {
  vi.resetModules();
  vi.resetAllMocks();
  proc = Object.assign(new EventEmitter(), {
    exitCode: null as number | null,
    kill: vi.fn(() => {
      queueMicrotask(() => {
        proc.exitCode = 0;
        proc.emit("exit", 0, null);
      });
      return true;
    }),
  });
  mocks.existsSync.mockReturnValue(true);
  mocks.spawnSync.mockReturnValue({ status: 0 });
  mocks.spawn.mockReturnValue(proc);
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true }));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

it("rebuilds the binary before testing current source", async () => {
  const { default: setup } = await import("../src/server.js");
  const teardown = await setup();
  await teardown();
  expect(mocks.spawnSync).toHaveBeenCalledWith(
    "cargo",
    expect.arrayContaining(["build", "-p", "pulse"]),
    expect.anything(),
  );
});

it("finishes teardown when the server has already exited", async () => {
  const { default: setup } = await import("../src/server.js");
  const teardown = await setup();
  proc.exitCode = 0;
  proc.emit("exit", 0, null);
  proc.kill.mockImplementation(() => false);
  let timer: ReturnType<typeof setTimeout> | undefined;
  const completed = await Promise.race([
    teardown().then(() => true),
    new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), 100);
    }),
  ]);
  clearTimeout(timer);
  expect(completed).toBe(true);
});

it("cleans up the child when health checking fails", async () => {
  vi.stubGlobal("fetch", vi.fn().mockImplementation(async () => {
    throw new Error("health connection failed");
  }));
  const { default: setup } = await import("../src/server.js");
  vi.useFakeTimers();
  try {
    const result = setup().then(() => false, () => true);
    await vi.advanceTimersByTimeAsync(16_000);
    expect(await result).toBe(true);
    expect(proc.kill).toHaveBeenCalled();
  } finally {
    vi.useRealTimers();
  }
});
