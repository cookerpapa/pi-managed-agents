"""Summarize private tail traces and PID/duration-only PG logs without application identifiers."""
import argparse
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timezone
import json
from math import ceil
from pathlib import Path
from statistics import median


def stats(values):
    if not values:
        return None
    v = sorted(values)
    return dict(n=len(v), p50=round(median(v), 3), p95=round(v[ceil(.95*len(v))-1], 3),
                p99=round(v[ceil(.99*len(v))-1], 3), maximum=round(v[-1], 3))


def log_events(path):
    result = defaultdict(list)
    if path == "-":
        return result
    for line in Path(path).read_text().splitlines():
        stamp, pid, duration = line.split("\t")
        ms = datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc).timestamp()*1000
        result[int(pid)].append((ms, float(duration)))
    for records in result.values():
        records.sort()
    return result


def server_sample(spans, events):
    # Server logs truncate the timestamp to milliseconds. Drop ambiguous spans
    # altogether. Require all three Parse/Bind/Execute events on the same PID.
    grouped = defaultdict(list)
    for pid, kind, start, end in spans:
        grouped[pid].append((start, end, kind))
    pairs = defaultdict(list)
    for pid, qs in grouped.items():
        qs.sort()
        starts = [q[0] for q in qs]
        totals, ambiguous = defaultdict(list), set()
        lo = bisect_left(events[pid], (starts[0]-2, -1))
        hi = bisect_right(events[pid], (qs[-1][1]+2, float("inf")))
        for stamp, duration in events[pid][lo:hi]:
            i = bisect_right(starts, stamp+1)-1
            candidates = []
            while i >= 0 and qs[i][1] >= stamp-.25:
                candidates.append(i)
                i -= 1
            if len(candidates) == 1:
                totals[candidates[0]].append(duration)
            elif len(candidates) > 1:
                ambiguous.update(candidates)
        for i, logs in totals.items():
            if len(logs) == 3 and i not in ambiguous:
                start, end, kind = qs[i]
                pairs[kind].append((end-start, sum(logs)))
    return {"sampledCalls": len(spans), "sampledBackends": len(grouped),
            "uniqueComplete": sum(map(len,pairs.values())), "byKind": {
                k: {"clientMs": stats([a for a,b in v]), "serverProtocolMs": stats([b for a,b in v])}
                for k,v in pairs.items()}}


def original_stage(path):
    report=json.loads((Path(path)/"report.json").read_text())
    assert report["cleaned"]
    t=next(t for t in report["tests"] if t["label"]=="fresh-160-60")
    peak=max(t["samples"],key=lambda s:s["lag"])
    return {"completed":t["completed"],"unissued":t["dropped"],"peakSample":peak,
            "claimP95":t["submitToClaimMs"]["p95"],"finishProducerAckP95":t["finishProducerAckMs"]["p95"],
            "createdToHandlerP95":t["finishToHandlerMs"]["p95"],"finishHandlerP95":t["finishPgMs"]["p95"],
            "terminalP95":t["submitToTerminalMs"]["p95"],"handlerByKind":t["projection"]["byKind"]}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output");parser.add_argument("original");parser.add_argument("--plans")
    parser.add_argument("--case",action="append",nargs=2,required=True,metavar=("PRIVATE_DIR","PG_TSV"))
    args=parser.parse_args()
    results=[]; sources={}
    for folder,log in args.case:
        report=json.loads((Path(folder)/"report.json").read_text())
        assert report["cleaned"] and report["wakePoliciesRemoved"] and not report.get("error")
        events=log_events(log)
        sample=Path(folder).name
        sources[sample]={"baseRevision":report["revision"],"actorBundles":report["actorHashes"]}
        for t in report["tests"]:
            if not t.get("tail"):continue
            b=t["tail"]; c=b["counts"]; ns=b["nativeSamples"]
            assert t["endKafkaLag"]==0 and t["completed"]+t["dropped"]==t["count"]
            assert c["messages"]==7*t["completed"] and b["phases"]["all"]["total"]["count"]==t["completed"]
            assert b["phases"]["all"]["total"]["p95"]==t["submitToTerminalMs"]["p95"]
            x=b["phases"]["example"]
            assert abs(x["total"]-sum(x[k] for k in ("claim","worker","untilReceive","local","terminal")))<.01
            switches=[]
            for z in b.get("windows",[]):
                seen=[s["fetchedNotPassed"] for s in ns if z["from"]<=s["at"]<z["from"]+t["switchMs"]]
                switches.append({**{k:z[k] for k in ("size","messages","calls","done")}, "nativeGap":seen})
            results.append({"sample":sample,"label":t["label"],"rate":t["rate"],"poll":t.get("pollSize","alternating"),
                "sink":t["sink"],"completed":t["completed"],"unissued":t["dropped"],"completedPerSecond":t["runsPerSecond"],
                "terminalP95":t["submitToTerminalMs"]["p95"],"claimP95":t["submitToClaimMs"]["p95"],
                "peakLag":t["peakKafkaLag"],"peakJSQueue":t["peakLocalQueue"],"peakPoolWaiters":t["peakPoolWaiting"],
                "nativeFetchedNotPassed":stats([s["fetchedNotPassed"] for s in ns]),
                "nativeStableNotFetched":stats([s["stableNotFetched"] for s in ns]),
                "polls":c["calls"],"delivered":c["messages"],"pauseTransitions":c["pauses"],
                "projectorSQL":t["projection"]["queries"],"endLag":0,
                "hostCalibrationMs":stats([s["calibrationMs"] for s in t["samples"] if s.get("calibrationMs") is not None]),
                "tailCount":b["phases"]["slowCount"],"sameRunExampleMs":{k:round(v,3) for k,v in x.items()},
                "slowestFivePercentMeanMs":{k:round(v["mean"],3) for k,v in b["phases"]["slowestFivePercent"].items()},
                "allPhaseP95Ms":{k:round(v["p95"],3) for k,v in b["phases"]["all"].items()},
                "pg":server_sample(b["sql"],events),"windows":switches,
                **({"predecessor":t["verified"]["predecessor"]} if "predecessor" in t["verified"] else {})})
    output={"original":original_stage(args.original),"sources":sources,"observations":results}
    if args.plans:
        output["plans"]=json.loads(Path(args.plans).read_text())
    def encode(value):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    metadata = [f'  "{k}": {encode(v)},' for k, v in output.items() if k != "observations"]
    lines = ["{", *metadata, '  "observations": [',
             ",\n".join("    " + encode(row) for row in results), "  ]", "}", ""]
    Path(args.output).write_text("\n".join(lines))


if __name__=="__main__":
    main()
