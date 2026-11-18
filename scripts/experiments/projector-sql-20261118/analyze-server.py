import json, re, sys, pathlib, datetime, collections, bisect, itertools
root = pathlib.Path(sys.argv[1])
m = json.load(open(root / 'load/report.json'))['measurements'][0]
a = m['startedAt']
b = a + m['summary']['elapsedMs']
ids = {x.get('runId') for x in m['load']['results'] if x.get('runId')}
queries = collections.defaultdict(list)
templates = {}
projections, handlers = [], []
for l in open(root / 'runtime.log'):
    try:
        r = json.loads(l)
    except ValueError:
        continue
    if r.get('event') == 'diag.sql.statement':
        templates[r['id']] = r['sql']
    if r.get('event') == 'diag.sql.projection' and r.get('runId') in ids:
        projections.append(r)
    if r.get('event') == 'diag.sql.handler':
        handlers.append(r)
sessions = {r['sessionId'] for r in projections}
projection_pids = {s['pid'] for r in projections for s in r['spans'] if s['type'] == 'query'}
handlers = [r for r in handlers if r['sessionId'] in sessions]
if handlers:
    a = min(x['submittedAt'] for x in m['load']['results'])
    b = max(r['at'] + r['durationMs'] for r in handlers if r['kind'] == 'work_finished')
for r in projections + handlers:
    for s in r['spans']:
        if s['type'] == 'query' and s['pid'] in projection_pids and (not handlers or a <= r['at'] + s['end'] <= b):
            queries[s['pid']].append({**s, 'begin': r['at'] + s['start'], 'finish': r['at'] + s['end'], 'server': 0, 'serverLogs': 0})
for vs in queries.values():
    vs.sort(key=lambda x: x['begin'])
starts = {pid: [x['begin'] for x in qs] for pid, qs in queries.items()}
latest_end = {pid: list(itertools.accumulate((x['finish'] for x in qs), max)) for pid, qs in queries.items()}
total = 0
matched = 0
unmatched = 0
ambiguous = 0
events = 0
logged_pids = set()
pattern = re.compile('(\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d+) UTC \\[(\\d+)\\].*duration:\\s*([\\d.]+) ms')
for line in open(root / 'postgres.log'):
    mt = pattern.search(line)
    if not mt:
        continue
    at = datetime.datetime.fromisoformat(mt[1]).replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
    pid = int(mt[2])
    duration = float(mt[3])
    if pid not in queries or not a <= at <= b:
        continue
    total += duration
    events += 1
    logged_pids.add(pid)
    index = bisect.bisect_right(starts[pid], at + 1.0)
    first = bisect.bisect_left(latest_end[pid], at - 1, 0, index)
    candidates = [q for q in queries[pid][first:index] if q['finish'] >= at - 1]
    if len(candidates) != 1:
        if not candidates:
            unmatched += 1
        else:
            ambiguous += 1
        continue
    q = candidates[0]
    q['server'] += duration
    q['serverLogs'] += 1
    matched += 1
groups = collections.defaultdict(list)
for qs in queries.values():
    for q in qs:
        if q['serverLogs']:
            groups[q['id']].append(q)
rows = []
for k, qs in groups.items():
    rows.append({'id': k, 'queries': len(qs), 'clientTotalMs': sum((q['end'] - q['start'] for q in qs)), 'serverTotalMs': sum((q['server'] for q in qs)), 'sql': templates.get(k, '')})
rows.sort(key=lambda x: x['serverTotalMs'], reverse=True)
out = {'serverDurationMs': total, 'durationEvents': events, 'matchedEvents': matched, 'unmatchedEvents': unmatched, 'ambiguousEvents': ambiguous, 'queryGroups': rows, 'method': 'Same-PID timestamp match, 1ms clock/rounding tolerance. Parse/Bind/Execute server duration events are separately summed. Ambiguous events are excluded from per-query groups.'}
if handlers:
    assert logged_pids == projection_pids and matched + unmatched + ambiguous == events
    q = [query for pid in logged_pids for query in queries[pid]]
    out.update(projectionPoolBackends=len(logged_pids), observedClientCalls=len(q),
               clientQueryMs=sum(x['finish'] - x['begin'] for x in q),
               uniquelyAssignedServerMs=sum(x['serverTotalMs'] for x in rows))
(root / 'server-analysis.json').write_text(json.dumps(out, indent=2) + '\n')
print({k: v for k, v in out.items() if k != 'queryGroups'})
for x in rows[:18]:
    print(x['id'], x['queries'], round(x['clientTotalMs'] / x['queries'], 3), round(x['serverTotalMs'] / x['queries'], 3), round(x['serverTotalMs']), x['sql'][:180])
