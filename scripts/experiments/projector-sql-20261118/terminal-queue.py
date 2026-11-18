"""Attribute the local wait of each terminal record to earlier records on its partition."""
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path


def read(path):
    return json.loads(path.read_text())


def quantiles(values):
    ordered = sorted(values)
    return {
        "mean": round(statistics.mean(ordered), 2),
        "p50": round(ordered[math.ceil(0.5 * len(ordered)) - 1], 2),
        "p95": round(ordered[math.ceil(0.95 * len(ordered)) - 1], 2),
        "max": round(ordered[-1], 2),
    }


def interval_union(intervals):
    total, through = 0, -math.inf
    for start, end in sorted(intervals):
        total += max(0, end - max(start, through))
        through = max(through, end)
    return total


def split_wait(spans, start, end):
    by_type = defaultdict(list)
    for a, b, kind in spans:
        if a < end and b > start:
            by_type[kind].append((max(a, start), min(b, end)))
    sql = interval_union(by_type["query"])
    pool = interval_union(by_type["acquire"])
    measured = interval_union(by_type["query"] + by_type["acquire"])
    return {
        "sqlCallback": measured - pool,
        "poolCheckout": measured - sql,
        "sqlAndPoolOverlap": sql + pool - measured,
        "handlerOther": end - start - measured,
    }


def category(row):
    if row["kind"] != "durable_commit":
        return ".".join(filter(None, [row["kind"], row.get("subtype")]))
    writes = row["writes"]
    for name in ("conversation", "entry:pi.system", "entry:pi.user", "entry:pi.assistant"):
        if name in writes:
            return name
    if writes == ["document.change"]:
        return "document.change.withSql" if row["queries"] else "document.change.withoutSql"
    return "+".join(writes)


def traces(directory, runs):
    readers, outer, inner, sessions = {}, {}, {}, {}
    deliveries = Counter()
    all_outer = []
    for line in (directory / "runtime.log").open():
        if not line.startswith("{"):
            continue
        event = json.loads(line)
        tag = event.get("event")
        if tag == "diag.reader.record":
            position = event["p"], int(event["o"])
            readers[position] = event
            deliveries[position] += 1
        if tag == "diag.sql.handler":
            all_outer.append(event)
        if tag == "diag.sql.projection" and event.get("runId") in runs:
            key = (event["partition"], int(event["offset"]))
            assert key not in inner
            inner[key] = event
            run = sessions.setdefault(event["sessionId"], event["runId"])
            assert run == event["runId"]
    assert len(sessions) == len(runs) and set(sessions.values()) == runs
    for event in all_outer:
        if event["sessionId"] in sessions:
            key = (event["partition"], int(event["offset"]))
            assert key not in outer
            outer[key] = event
    assert not all_outer or inner.keys() <= outer.keys()
    by_partition, terminal, pids = defaultdict(list), {}, set()
    doc_kinds = {}
    for key, row in sorted((outer or inner).items()):
        assert deliveries[key] == 1, "Queue attribution requires a single observed delivery"
        core = inner.get(key)
        layers = [row, core] if outer and core else [row]
        assert not any(layer["failed"] or layer["truncated"] for layer in layers)
        spans, queries = [], 0
        for layer in layers:
            for s in layer["spans"]:
                assert s["end"] >= s["start"] and "error" not in s
                spans.append((layer["at"] + s["start"], layer["at"] + s["end"], s["type"]))
                queries += s["type"] == "query"
                if core is layer and s.get("pid"):
                    pids.add(s["pid"])
        names = []
        for doc in row.get("documents") or []:
            address = (row["sessionId"], doc[0])
            if len(doc) == 2:
                doc_kinds[address] = doc[1]
            names.append(doc_kinds.get(address, "unknown"))
        record = {
            **row, "spans": spans, "queries": queries,
            "commits": sum(s.get("command") == "commit" for layer in layers for s in layer["spans"]),
            "docs": "+".join(names), "offset": key[1],
        }
        record["category"] = category(record)
        by_partition[key[0]].append(record)
        if record["kind"] == "work_finished":
            rid = record["runId"]
            assert rid not in terminal
            terminal[rid] = key
    assert terminal.keys() == runs
    return by_partition, terminal, sessions, readers, pids, bool(outer)


def pg_activity(directory, first, last, pids):
    data = read(directory / "pg-samples.json")
    assert not data["errors"]
    states, observations = Counter(), 0
    for sample in data["samples"]:
        if first <= sample["sampledAt"] <= last:
            observations += 1
            for row in sample["rows"]:
                if row["pid"] in pids:
                    states[" / ".join(filter(None, [row["state"], row["wait_event_type"], row["wait_event"]]))] += 1
    assert observations and states, "No independent PG observations in the timed wave"
    return {"samples": observations, "nonIdleBackendObservations": dict(states.most_common())}


def adjacent_bootstrap(partitions, readers):
    # Kafka transaction markers can leave integer offsets between successive
    # delivered records. Rank the reader's actual deliveries instead.
    names = ("conversation", "document.create", "document.create+document.change")
    positions = defaultdict(dict)
    for partition, records in partitions.items():
        ranks = {key: i for i, key in enumerate(sorted(k for k in readers if k[0] == partition))}
        for r in records:
            if r["category"] in names:
                session = positions[r["sessionId"]]
                assert r["category"] not in session
                session[r["category"]] = ranks[partition, r["offset"]]
    assert all(set(session) == set(names) for session in positions.values())
    ordered = [[r[name] for name in names] for r in positions.values()]
    assert all(a < b < c for a, b, c in ordered)
    return {
        "sessions": len(ordered),
        "adjacentWorldAndRun": sum(c == b + 1 for _, b, c in ordered),
        "adjacentRootWorldRun": sum(c == a + 2 for a, _, c in ordered),
    }


def inspect(path):
    report = read(path)
    assert report["accepted"] and report["cleanupConfirmed"] and not report["realProviderRequests"]
    m, = report["measurements"]
    runs = {r["runId"] for r in m["load"]["results"] if r["success"]}
    assert len(runs) == m["load"]["maximumInFlight"] == 192 and report["postLoad"]["lastKafkaLag"] == 0
    assert m["profile"]["outputBytes"] == 4096 and m["profile"]["firstByteMs"] == 0
    directory = path.parent.parent
    observation = read(directory / "observation-private.json")
    assert observation["poolSize"] in (12, 32) and observation["replicas"] == 1 and observation["phase"] == "restored"
    assert observation["poolSize"] == 12 or observation["serverDuration"]
    partitions, terminal, session_runs, readers, pids, whole = traces(directory, runs)
    wave_start = min(r["submittedAt"] for r in m["load"]["results"])
    wave_end = max(r["at"] + r["durationMs"] for records in partitions.values() for r in records
                   if r["kind"] == "work_finished")
    by_kind, phases, identities = defaultdict(Counter), Counter(), Counter()
    docs, runs_by_partition = defaultdict(Counter), Counter(part for part, _ in terminal.values())
    control_timing = defaultdict(Counter)
    per_run = []
    for part, records in partitions.items():
        for r in records:
            in_burst = r["at"] < wave_end and r["at"] + r["durationMs"] >= wave_start
            if r["kind"] == "control_change":
                own = session_runs[r["sessionId"]]
                if not in_burst:
                    phase = "outsideBurst"
                elif r["offset"] < terminal[own][1]:
                    phase = "beforeOwnTerminal"
                else:
                    phase = "afterOwnTerminalDuringBurst"
                control_timing[r["category"]][phase] += 1
            if not in_burst:
                continue
            counts = by_kind[r["category"]]
            counts["records"] += 1
            counts["pgCalls"] += r["queries"]
            counts["commits"] += r["commits"]
            if r["docs"]:
                docs[r["category"]][r["docs"]] += 1
    for rid, key in terminal.items():
        reader = readers[key]
        lo, hi = reader["received"], reader["started"]
        assert hi >= lo
        through = lo
        ahead = Counter()
        foreign = set()
        for r in partitions[key[0]]:
            if r["offset"] >= key[1]:
                break
            first, last = max(through, r["at"]), min(hi, r["at"] + r["durationMs"])
            if last <= first:
                continue
            duration = last - first
            through = last
            group = by_kind[r["category"]]
            group["queuedMs"] += duration
            for phase, value in split_wait(r["spans"], first, last).items():
                phases[phase] += value
            same = session_runs[r["sessionId"]] == rid
            owner = "sameSession" if same else "otherSessions"
            identities[owner] += duration
            ahead[owner] += 1
            if not same:
                foreign.add(r["sessionId"])
        per_run.append({
            "waitMs": hi - lo, "overlappingRecords": ahead.total(),
            "ownRecords": ahead["sameSession"], "otherRecords": ahead["otherSessions"],
            "otherSessions": len(foreign), "partitionSessions": runs_by_partition[key[0]],
        })
    queued = sum(r["waitMs"] for r in per_run)
    phases["outsideMeasuredHandler"] = queued - sum(phases.values())
    assert abs(sum(identities.values()) + phases["outsideMeasuredHandler"] - queued) < .1 and queued > 0
    def pct(value):
        return round(100 * value / queued, 2)

    mix = {
        kind: {
            "records": counts["records"], "pgCalls": counts["pgCalls"],
            "transactionsCommitted": counts["commits"], "queuePercent": pct(counts["queuedMs"]),
        } for kind, counts in sorted(by_kind.items())
    }
    ahead_metrics = ("overlappingRecords", "ownRecords", "otherRecords", "otherSessions")
    return {
        "runs": len(runs), "poolSize": observation["poolSize"], "wholeHandler": whole,
        "runsPerSecond": round(m["summary"]["successfulRunsPerSecond"], 2),
        "sampledPeakLag": m["resources"]["maximumObservedKafkaLag"],
        "drainedLag": report["postLoad"]["lastKafkaLag"],
        "queueMs": quantiles([r["waitMs"] for r in per_run]),
        "phasesPercent": {k: pct(v) for k, v in phases.items()},
        "identityPercent": {k: pct(v) for k, v in identities.items()},
        "recordMix": mix,
        "documentsByRecordShape": dict(docs),
        "controlMessageTiming": dict(control_timing),
        "bootstrapAdjacency": adjacent_bootstrap(partitions, readers),
        "waitingBehind": {k: quantiles([r[k] for r in per_run]) for k in ahead_metrics},
        "medianExample": sorted(per_run, key=lambda r: r["waitMs"])[(len(per_run) - 1) // 2],
        "assignedPartitionsWithRuns": len(partitions), "sessionsPerUsedPartition": quantiles(list(runs_by_partition.values())),
        "pgActivity": pg_activity(directory, wave_start, wave_end, pids),
    }


if __name__ == "__main__":
    result = {}
    for argument in sys.argv[2:]:
        label, path = argument.split("=", 1)
        result[label] = inspect(Path(path))
    Path(sys.argv[1]).write_text(json.dumps(result, indent=2) + "\n")
