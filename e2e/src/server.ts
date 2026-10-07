import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { HOST, PORT } from "./constants.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const E2E_DIR = join(__dirname, "..");
const REPO_ROOT = join(E2E_DIR, "..");
const BINARY = join(REPO_ROOT, process.platform === "win32" ? "pulse.exe" : "pulse");
const CONFIG = join(E2E_DIR, "config.json");

let proc: ChildProcess | undefined;

function buildBinaryIfMissing(): void {
  if (existsSync(BINARY)) return;
  console.log("[e2e] pulse binary not found, building with `go build`...");
  const res = spawnSync("go", ["build", "-o", BINARY, "."], {
    cwd: REPO_ROOT,
    stdio: "inherit",
  });

  if (res.status !== 0) {
    throw new Error(`failed to build pulse binary (go build exit ${res.status})`);
  }
}

async function waitForHealth(timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const url = `http://${HOST}:${PORT}/health`;
  while (Date.now() < deadline) {
    if (proc?.exitCode != null) {
      throw new Error(`pulse exited early with code ${proc.exitCode}`);
    }
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await sleep(150);
  }
  throw new Error(`pulse did not become healthy within ${timeoutMs}ms`);
}

export default async function setup(): Promise<() => Promise<void>> {
  buildBinaryIfMissing();

  proc = spawn(BINARY, ["-config", CONFIG, "-port", String(PORT)], {
    cwd: REPO_ROOT,
    stdio: ["ignore", "inherit", "inherit"],
    env: { ...process.env, PULSE_PORT: String(PORT) },
  });

  proc.on("exit", (code, signal) => {
    if (code != null && code !== 0) {
      console.error(`[e2e] pulse exited with code ${code} signal ${signal}`);
    }
  });

  await waitForHealth();
  console.log(`[e2e] pulse healthy on ${HOST}:${PORT}`);

  return async () => {
    if (!proc) return;
    const p = proc;
    proc = undefined;
    await new Promise<void>((resolve) => {
      p.once("exit", () => resolve());
      p.kill("SIGTERM");

      setTimeout(() => p.kill("SIGKILL"), 5_000).unref();
    });
  };
}
