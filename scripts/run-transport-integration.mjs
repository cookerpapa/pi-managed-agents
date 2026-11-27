import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
// Explicit isolated RF3/PG coverage for cases skipped by the default suite.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { testKafka } from "./lib/test-kafka.mjs";
import { testPostgres } from "./lib/test-postgres.mjs";

const historyRoot = await mkdtemp(join(tmpdir(), "pma-worker-ssd-"));
let kafka, postgres;
try {
  kafka = await testKafka({});
  postgres = await testPostgres({});
  const files = [
    "packages/event-log/test/publication.integration.test.ts",
    "packages/event-log/test/rolling-reader.integration.test.ts",
    "packages/event-log/test/prefetch.integration.test.ts",
    "scripts/experiments/minimal-session-storage/ready-projection.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/root-message-history.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/bash-intent.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/segment-fork.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/independent-session-leases.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/segment-wake.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/segment-warm-history.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/segment-inputs.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/agent-configuration.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/admission-retention.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/conversation-world.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/child-outcomes.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/delegated-runs.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/delegated-history.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/projected-context.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/direct-delegation-source.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/named-executor.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/delegated-handoff.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/family-inboxes.integration.spec.mjs",
    "scripts/experiments/minimal-session-storage/sealed-family.integration.spec.mjs",
    "packages/event-log/test/tool-reply-recovery.integration.test.ts",
    "packages/event-log/test/tool-catalog.integration.test.ts",
    "packages/runtime-core/test/kafka-topic-policy.integration.test.ts",
    "packages/control-plane/test/session-publication.integration.test.ts",
    "packages/control-plane/test/ordered-binding-views.integration.test.ts",
    "packages/control-plane/test/subagent-views.integration.test.ts",
    "packages/control-plane/test/agent-stop-binding-closure.integration.test.ts",
  ];
  const child = spawn(
    process.execPath,
    [
      "node_modules/vitest/vitest.mjs",
      "run",
      ...files,
      "--maxWorkers=1",
      "--no-file-parallelism",
      "--exclude=**/.cache/**",
      "--testTimeout=180000",
      "--hookTimeout=60000",
    ],
    {
      cwd: fileURLToPath(new URL("..", import.meta.url)),
      env: {
        ...process.env,
        PI_CLOUD_WORKER_HISTORY_CACHE_DIR: "",
        PI_CLOUD_WORKER_HISTORY_CACHE_ROOT: historyRoot,
        TSX_TSCONFIG_PATH:
          process.env.TSX_TSCONFIG_PATH ??
          fileURLToPath(new URL("../packages/control-plane/tsconfig.json", import.meta.url)),
        PI_CLOUD_POSTGRES_INTEGRATION_URL: postgres.url,
        PI_CLOUD_KAFKA_INTEGRATION_BROKERS: kafka.brokers.join(","),
        PI_CLOUD_ISOLATED_TOOL_CATALOG_CHECK: "1",
      },
      stdio: "inherit",
    },
  );
  process.exitCode = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code) => resolve(code ?? 1));
  });
} finally {
  const closed = await Promise.allSettled([
    kafka?.close(),
    postgres?.close(),
    rm(historyRoot, { recursive: true, force: true }),
  ]);
  const errors = closed.filter((value) => value.status === "rejected").map((value) => value.reason);
  if (errors.length) throw new AggregateError(errors, "Owned transport fixture cleanup failed");
}
