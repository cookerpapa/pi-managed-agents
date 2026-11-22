// Operator-only endpoint map. Only the execution actor can ask this driver to
// submit an admitted Tool; results go through Guest HTTPS and the Worker mailbox.
import assert from "node:assert/strict";
import { withFamilyGuest } from "./retained-family-cube.mjs";
import { installContinuationEnvironments } from "./continuation-cases.mjs";
import { studyCubeConfig } from "./study-cube-config.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";

const root = "/workspace/game",
  client = root + "/client";
export async function installGuestResources(db) {
  await installWorkspaceRoutes(db);
  await installContinuationEnvironments(db);
  await db.query(
    "create table study_guest_targets(workspace text primary key, target uuid unique not null)",
  );
  await db.query(
    "create table study_guest_attachments(binding uuid primary key default gen_random_uuid(), target uuid references study_guest_targets(target) not null, cwd text not null, unique(target,cwd))",
  );
}
export async function runGuestContinuation(wave, db, endpoints) {
  await installGuestResources(db);
  const config = await studyCubeConfig();
  await withFamilyGuest(config, (first) =>
    withFamilyGuest(config, async (second) => {
      const locations = [
        { guest: first, workspace: "first", cwd: root, label: "root" },
        { guest: first, workspace: "first", cwd: client, label: "client" },
        { guest: second, workspace: "second", cwd: client, label: "other-client" },
      ];
      try {
        for (const [workspace, guest] of [
          ["first", first],
          ["second", second],
        ]) {
          const target = guest.allocationId;
          endpoints.set(target, guest);
          await db.query("insert into study_guest_targets values($1,$2)", [workspace, target]);
        }
        for (const loc of locations) {
          const { guest, cwd, label } = loc;
          await guest.command(
            `install -d -m 0755 -o 1000 -g 1000 ${cwd} && printf '${label}-%s\\n' "$(cat /proc/sys/kernel/random/uuid)" > ${cwd}/marker`,
          );
          loc.marker = await guest.command("cat " + cwd + "/marker");
          loc.runs = [];
        }
        assert.equal(new Set(locations.map((l) => l.marker)).size, 3);
        const environmentCases = locations.map(({ workspace, cwd }) => ({
          workspace,
          cwd,
          processGeneration: 1,
        }));
        for (const switchAt of [undefined, 1, 2]) {
          const result = await wave({
            label: "guest-continuation-" + (switchAt ?? "warm"),
            environmentCases,
            guestTools: true,
            integrated: true,
            cacheMode: "prefer",
            wakeMode: "directed",
            cohort: 1,
            seedBytes: 320 * 1024,
            offline: ["w1"],
            ...(switchAt ? { switchAt } : { joinIdleAt: 1 }),
            count: 3,
            concurrency: 1,
            verifyFull: true,
            sink: "single",
            prefixWidth: 16,
            projectionPolicy: "recorded-history",
            slots: 8,
            batch: 8,
            omitNotify: true,
            probeUnlisted: true,
            windowMs: 5,
            tickMs: 10000,
          });
          for (const [i, trace] of result.environmentTrace.entries()) {
            const loc = locations[i];
            assert.equal(trace.worker, switchAt && i >= switchAt ? "w1" : "w0");
            assert.equal(trace.prefix, i === 0 || i === switchAt ? "cold" : "hit");
            assert.equal(trace.readBytes === 0, trace.prefix === "hit");
            const prompt = [
              "Current working directory: " + loc.cwd,
              ...(i === 1
                ? ["Working directory changed from " + root + " to " + client + "."]
                : []),
              ...(i === 2
                ? ["Workspace changed. Inspect the current files before reusing earlier paths."]
                : []),
            ].join("\n");
            assert.equal(trace.prompt, prompt);
            assert.equal(trace.bash, loc.cwd + "\n" + loc.marker);
            loc.runs.push(trace.runId);
          }
          const {
            rows: [observation],
          } = await db.query(
            "select w.state#>>'{sandbox,continuityId}' as generation from runs r join conversation_worlds w on w.tenant_id=r.tenant_id and w.pi_session_id=r.session_id and w.conversation_id=1 where r.id=$1",
            [result.environmentTrace.at(-1).runId],
          );
          assert.equal(
            observation?.generation,
            second.allocationId,
            "The settled Guest Tool must record the original allocation",
          );
          assert(result.router.preferred >= (switchAt ? 1 : 2));
        }
        for (const loc of locations) {
          const log = await loc.guest.command("cat " + loc.cwd + "/executions");
          assert.deepEqual(log.split("\n"), loc.runs, "Physical Bash executions at this location");
        }
      } finally {
        endpoints.clear();
      }
    }),
  );
}
