"""Keep only PostgreSQL duration timestamps, backend PIDs, and durations on stdin."""
import re
import sys

duration = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) UTC \[(\d+)\].*?duration:\s*([\d.]+) ms")
for line in sys.stdin:
    match = duration.search(line)
    if match:
        print(*match.groups(), sep="\t")
