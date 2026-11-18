"""Publish aggregate timings only. Usage: python3 summarize-tail.py PRIVATE_ROOT OUTPUT.json"""
import collections
import json
import pathlib
import statistics
import sys

root, destination = map(pathlib.Path, sys.argv[1:])
def read(path):
    return json.loads(path.read_text())

def lines(path):
    if not path.exists():
        return
    for line in path.open():
        try:
            yield json.loads(line)
        except ValueError:
            continue

def sample_distribution(values):
    if not values:
        return None
    return {"samples": len(values), "median": round(statistics.median(values), 3),
            "minimum": round(min(values), 3), "maximum": round(max(values), 3)}

windows = list(lines(root / "host-processor.ndjson"))
calibration = list(lines(root / "host-calibration.ndjson"))
experiments = {}
for label in ["control", "two-owners", "grouped-one", "grouped-two", "control-r2",
              "grouped-counted", "grouped-faults", "grouped-two-generic", "grouped-two-r2",
              "two-owners-r2", "grouped-two-r3"]:
    directory = root / label
    overlay_file = directory / "observation-private.json"
    overlay = read(overlay_file) if overlay_file.exists() else None
    report = read(directory / ("load/report.json" if overlay else "report.json"))
    assert report["accepted"] and report["cleanupConfirmed"] and not report["realProviderRequests"], label
    if overlay:
        assert overlay["phase"] == "restored" and overlay["loadExit"] == 0, label
    instance = {"projectors": overlay["replicas"] if overlay else 1,
                "projectionConnectionsEach": overlay["poolSize"] if overlay else 12,
                "partitions": overlay.get("partitionsPerOwner") if overlay else [32],
                "candidate": bool(overlay and overlay["candidateProjection"]),
                "genericPlans": bool(overlay and overlay.get("genericPlans")),
                "sqlTracing": bool(overlay and not overlay["clean"]),
                "cleanupConfirmed": True, "stages": {}}
    if overlay:
        instance["overlayRestored"] = True
        instance["applicationSourceHashes"] = {
            k: v for k, v in overlay["sourceHashes"].items() if k.endswith(".ts")
        }
    instance["loadDriverSha256"] = report["sourceSha256"]["scripts/run-runtime-load.mjs"]
    for m in report["measurements"]:
        profile, s = m["profile"], m["summary"]
        if label == "grouped-faults":
            assert m["expectedOutcomesPassed"], profile["name"]
            instance["stages"][profile["name"]] = {"passed": True,
                **{k: v for k,v in m.get("evidence", {}).items() if k in
                   ("idlePartitionsReady","idleSnapshotMaximumMs","cancelled","lateReplyIgnored","physicalWrites","physicalCalls","automaticReplayClaimed")}}
            continue
        assert s["offered"] == s["completed"] + s["failed"] + s["dropped"]
        tail = m.get("tail", {})
        stage = {"offered": s["offered"], "accepted": s["accepted"], "completed": s["completed"],
                 "failed": s["failed"], "notLaunched": s["dropped"],
                 "maxInFlight": s["maximumInFlight"], "elapsedMs": round(s["elapsedMs"], 1),
                 "completeRunsPerSecond": round(s["successfulRunsPerSecond"], 3),
                 "PGSettlementP95Ms": s["settlementMs"]["p95"],
                 "peakObservedRunning":m["resources"]["peakActiveRootRuns"],
                 "modelRequests":m["provider"]["requests"]}
        if "physicalToolCalls" in tail:
            stage["physicalToolCalls"] = tail["physicalToolCalls"]
        for key in ["submitToVisibleTerminalMs", "terminalCreationToSettlementMs", "beforeProviderMs", "afterProviderMs"]:
            if key in tail:
                stage[key] = {n:tail[key][n] for n in ("count","p50","p95","p99")}
        concurrent = peak = 0
        points = [(r["startedAt"],1) for r in m["samples"]] + [(r["finishedAt"],-1) for r in m["samples"]]
        for _, delta in sorted(points):
            concurrent += delta
            peak = max(peak, concurrent)
        stage["peakConcurrentModelRequests"] = peak
        start, end = m["startedAt"], m["startedAt"] + s["elapsedMs"]
        stage["hostProcessorPerformance"] = sample_distribution([r["performance"] for r in windows if start <= r["at"] <= end])
        stage["fixedHashWallMs"] = sample_distribution([r["elapsedMs"] for r in calibration if start <= r["at"] <= end])
        instance["stages"][profile["name"]] = stage
    experiments[label] = instance

observed = root / "grouped-counted"
report = read(observed / "load/report.json")
ids = {r["runId"] for r in report["measurements"][0]["load"]["results"]}
records = [r for r in lines(observed / "runtime.log")
           if r.get("event") == "diag.sql.projection" and r.get("runId") in ids]
assert all(not r["failed"] and not r["truncated"] for r in records)
kind_counts = collections.defaultdict(list)
for r in records:
    k = r["kind"]
    if r.get("writes") == ["task:pi.generation"]:
        k = "generationTaskOnly"
    elif "entry:pi.assistant" in (r.get("writes") or []):
        k = "assistantEntryCommit"
    kind_counts[k].append(sum(s["type"] == "query" for s in r["spans"]))
sql = {k: {"records":len(v), "calls":sum(v), "minCalls":min(v), "maxCalls":max(v)} for k,v in kind_counts.items()}
summary = {"format":"pma.projection-tail.v1", "servingRelease":report["serving"],
           "scope":"Single-host development PG / RF3 Kafka / real Workers and Session client / deterministic model. Time spans and client-visible SSE percentiles join observations by Run. PG settlement is timestamped within its terminal transaction.",
           "cases":experiments, "candidateSQL":{"runs":len(ids),"records":len(records),"byKind":sql,
           "callsPerRun":round(sum(sum(v) for v in kind_counts.values()) / len(ids),3)}}
destination.write_text(json.dumps(summary, indent=2)+"\n")
