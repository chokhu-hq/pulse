import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["regression/**/*.test.ts"],
    testTimeout: 2_000,
  },
});
