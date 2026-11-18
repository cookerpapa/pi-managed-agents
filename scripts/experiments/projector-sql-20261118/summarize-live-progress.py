"""Summarize owned, synchronized 192-client traces without Session or model data."""
import hashlib
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path


def distribution(values):
    ordered = sorted(values)
    return {
        "mean": round(statistics.mean(ordered), 2),
        "p50": round(ordered[math.ceil(len(ordered) * 0.5) - 1], 2),
        "p95": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 2),
        "max": round(ordered[-1], 2),
    }


def queue_overlap(terminal, read, handlers):
    queued = covered = assistant = 0
    for (partition, offset), _ in terminal.values():
        record = read[partition, offset]
        start, end = record["received"], record["started"]
        queued += end - start
        through = start
        for began, finished, position, has_assistant in sorted(handlers[partition]):
            if position >= offset or began >= end or finished <= through:
                continue
            duration = min(end, finished) - max(through, began)
            covered += duration
            if has_assistant:
                assistant += duration
            through = min(end, finished)
    if not queued:
        return None
    return {"earlierSamePartitionHandlerPercent": round(100 * covered / queued, 2),
            "earlierAssistantBatchPercent": round(100 * assistant / queued, 2)}


def summarize(path):
    report = json.loads(path.read_text())
    assert report["accepted"] and report["cleanupConfirmed"] and not report["realProviderRequests"]
    measurement, = report["measurements"]
    runs = {r["runId"]: r for r in measurement["load"]["results"] if r["success"]}
    providers = {s["planId"]: s for s in measurement["samples"]}
    assert len(runs) == len(providers) == measurement["load"]["maximumInFlight"] == 192
    assert measurement["profile"]["outputBytes"] == 4096 and measurement["profile"]["firstByteMs"] == 0
    observer = json.loads((path.parent.parent / "observation-private.json").read_text())
    assert observer["phase"] == "restored" and observer["replicas"] == 1 and observer["poolSize"] == 12
    read, terminal = {}, {}
    handlers = defaultdict(list)
    live_calls, native_calls = Counter(), 0
    entry_calls = {}
    for line in (path.parent.parent / "runtime.log").open():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("event") == "diag.reader.record":
            read[row["p"], int(row["o"])] = row
        if row.get("event") != "diag.sql.projection" or row.get("runId") not in runs:
            continue
        assert not row["failed"] and not row["truncated"]
        queries = [s for s in row["spans"] if s["type"] == "query"]
        handlers[row["partition"]].append((row["at"], row["at"] + row["durationMs"],
            int(row["offset"]), "entry:pi.assistant" in (row.get("writes") or [])))
        if row["kind"] == "durable_commit":
            native_calls += len(queries)
            if row["writes"] == ["document.change"]:
                live_calls[len(queries)] += 1
            entries = [w for w in row["writes"] if w.startswith("entry:")]
            for role in set(entries):
                entry_calls.setdefault(role, Counter())[len(queries)] += 1
        if row["kind"] == "work_finished":
            commit, = [s for s in queries if s.get("command") == "commit"]
            assert "error" not in commit and row["runId"] not in terminal
            terminal[row["runId"]] = ((row["partition"], int(row["offset"])), row["at"] + commit["end"])
    assert terminal.keys() == runs.keys()
    lag = {}
    first_post = min(r["submittedAt"] for r in runs.values())
    for sample in measurement["metrics"]:
        kafka = sample["kafka"]
        lag[kafka["observedAt"] - first_post] = kafka["lag"]
    return {
        "runs": len(runs),
        "diagnosticSourceHash": hashlib.sha256(json.dumps(observer["sourceHashes"], sort_keys=True).encode()).hexdigest(),
        "workloadMs": round(measurement["summary"]["elapsedMs"], 2),
        "runsPerSecond": round(measurement["summary"]["successfulRunsPerSecond"], 2),
        "lastPostMs": max(r["submittedAt"] for r in runs.values()) - first_post,
        "lastClientConfirmationMs": max(r["observedAt"] for r in runs.values()) - first_post,
        "inFlightDocChangeCalls": dict(sorted(live_calls.items())),
        "nativeCallsPerRun": round(native_calls / len(runs), 2),
        "callsPerEntryBatch": {kind: dict(sorted(c.items())) for kind, c in entry_calls.items()},
        **({"storage": measurement["textStorage"]} if "textStorage" in measurement else {}),
        "modelFinishToTerminalCommitMs": distribution([
            finished - providers[runs[run]["planId"]]["finishedAt"]
            for run, (_, finished) in terminal.items()
        ]),
        "modelFinishToTerminalReceiptMs": distribution([
            read[pos]["received"] - providers[runs[run]["planId"]]["finishedAt"]
            for run, (pos, _) in terminal.items()
        ]),
        "terminalLocalQueueMs": distribution([
            read[pos]["started"] - read[pos]["received"] for pos, _ in terminal.values()
        ]),
        "terminalHandlerStartToCommitMs": distribution([
            finished - read[pos]["started"] for pos, finished in terminal.values()
        ]),
        "terminalQueueOverlap": queue_overlap(terminal, read, handlers),
        "groupLagSamples": sorted(lag.items()),
        "sampledPeakLag": max(lag.values()),
        "drainedLag": report["postLoad"]["lastKafkaLag"],
        "cleaned": report["cleanupConfirmed"],
    }


if __name__ == "__main__":
    output, *reports = sys.argv[1:]
    assert len(reports) >= 2
    measurements = []
    for index, argument in enumerate(reports, 1):
        label, separator, path = argument.partition("=")
        measurements.append({"case": label if separator else f"trial-{index}", **summarize(Path(path if separator else argument))})
    Path(output).write_text(json.dumps(measurements, indent=2) + "\n")
