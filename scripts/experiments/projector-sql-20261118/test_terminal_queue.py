import runpy
import unittest
import json
import subprocess
import sys
import tempfile
from pathlib import Path

analysis = runpy.run_path(str(Path(__file__).with_name("terminal-queue.py")))


class QueueIntervals(unittest.TestCase):
    def test_nested_queries_do_not_double_count_shared_wait(self):
        spans = [(0, 4, "acquire"), (2, 7, "query"), (5, 8, "query"), (9, 13, "acquire")]
        parts = analysis["split_wait"](spans, 1, 11)
        self.assertEqual(parts, {"sqlCallback": 4, "poolCheckout": 3,
                                 "sqlAndPoolOverlap": 2, "handlerOther": 1})
        self.assertEqual(sum(parts.values()), 10)

    def test_adjacency_follows_deliveries_across_offset_gaps(self):
        def row(kind, offset):
            return {"category": kind, "offset": offset, "sessionId": "owned"}
        records = {1: [row("conversation", 10), row("document.create", 12),
                       row("document.create+document.change", 16)]}
        readers = {(1, i): {} for i in (10, 12, 16)}
        self.assertEqual(analysis["adjacent_bootstrap"](records, readers)["adjacentRootWorldRun"], 1)
        readers[1, 14] = {}  # Another delivered record really was in the way.
        self.assertEqual(analysis["adjacent_bootstrap"](records, readers)["adjacentWorldAndRun"], 0)
        combined = {1: [row("conversation", 10), row("document.create+document.create+document.change", 16)]}
        self.assertEqual(analysis["adjacent_bootstrap"](combined, readers)["combinedWorldAndRun"], 1)

    def test_server_matching_counts_outer_queries_and_leaves_ambiguous_events_unassigned(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "load").mkdir()
            measurement = {"startedAt": 100, "summary": {"elapsedMs": 30},
                           "load": {"results": [{"runId": "r", "submittedAt": 100}]}}
            (root / "load/report.json").write_text(json.dumps({"measurements": [measurement]}))
            rows = []
            for event, statement, start in [("diag.sql.projection", "inner", 1), ("diag.sql.handler", "outer", 5)]:
                rows.append({"event": event, "kind": "work_finished", "at": 100, "durationMs": 12,
                             "sessionId": "s", "runId": "r", "spans": [
                                 {"type": "query", "pid": 7, "id": statement, "start": start, "end": start+2}]})
            (root / "runtime.log").write_text("\n".join(map(json.dumps, rows)))
            (root / "postgres.log").write_text("".join(
                f"1970-01-01 00:00:00.{ms} UTC [7] LOG: duration: 0.5 ms\n"
                for ms in (102, 104, 106, 130)))
            subprocess.run([sys.executable, str(Path(__file__).with_name("analyze-server.py")), tmp],
                           check=True, capture_output=True)
            summary = json.loads((root / "server-analysis.json").read_text())
            self.assertEqual((summary["observedClientCalls"], summary["clientQueryMs"]), (2, 4))
            self.assertEqual((summary["durationEvents"], summary["matchedEvents"], summary["ambiguousEvents"]), (3, 2, 1))
            self.assertEqual(summary["uniquelyAssignedServerMs"], 1)


if __name__ == "__main__":
    unittest.main()
