import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Build and benchmark experiments may retain source copies. Only the
    // maintained checkout belongs to source/fault acceptance.
    exclude: [...configDefaults.exclude, "**/.cache/**"],
  },
});
