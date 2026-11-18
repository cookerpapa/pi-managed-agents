import json, sys, pathlib, collections, statistics
root = pathlib.Path(sys.argv[1])
r = json.load(open(root / 'load/report.json'))
m = r['measurements'][0]
ids = {v.get('runId') for v in m['load']['results'] if v.get('runId')}
rows = []
queries = {}
pools = {}
for line in open(root / 'runtime.log'):
    try:
        v = json.loads(line)
    except:
        continue
    if v.get('event') == 'diag.sql.projection' and v.get('runId') in ids:
        rows.append(v)
    if v.get('event') == 'diag.sql.statement':
        queries[v['id']] = v['sql']
    if v.get('event') == 'diag.sql.pool':
        pools[v['pool']] = v

def d(xs):
    if not xs:
        return {'count': 0}
    xs = sorted(xs)
    return {'count': len(xs), 'mean': round(statistics.mean(xs), 3), 'p50': round(xs[len(xs) // 2], 3), 'p95': round(xs[min(len(xs) - 1, int(len(xs) * 0.95))], 3), 'max': round(xs[-1], 3), 'total': round(sum(xs), 2)}

def union(spans):
    total = 0
    last = 0
    for s, e in sorted(spans):
        if e > last:
            total += max(0, e - max(last, s))
            last = e
    return total
sqls = collections.defaultdict(list)
acqs = []
breaks = []
for v in rows:
    qs = [(s['start'], s['end']) for s in v['spans'] if s['type'] == 'query']
    ps = [(s['start'], s['end']) for s in v['spans'] if s['type'] == 'acquire']
    breaks.append({'wall': v['durationMs'], 'queryUnion': union(qs), 'poolUnion': union(ps), 'other': max(0, v['durationMs'] - union(qs + ps)), 'queryCount': len(qs)})
    for s in v['spans']:
        if s['type'] == 'query':
            sqls[s['id']].append(s['end'] - s['start'])
        else:
            acqs.append(s)
out = {'accepted': r.get('accepted'), 'cleanup': r.get('cleanupConfirmed'), 'summary': m['summary'], 'records': len(rows), 'kinds': dict(collections.Counter((v['kind'] for v in rows))), 'truncated': sum((v['truncated'] for v in rows)), 'pools': pools, 'breakdown': {k: d([v[k] for v in breaks]) for k in ['wall', 'queryUnion', 'poolUnion', 'other', 'queryCount']}, 'acquire': d([s['end'] - s['start'] for s in acqs]), 'queryGroups': [{'id': k, **d(v), 'sql': queries.get(k)} for k, v in sorted(sqls.items(), key=lambda x: sum(x[1]), reverse=True)]}
(root / 'analysis.json').write_text(json.dumps(out, indent=2) + '\n')
print('workload', m['summary']['successfulRunsPerSecond'], 'runs', m['summary']['completed'], 'errors', m['summary']['errors'])
print('records', out['records'], 'kinds', out['kinds'], 'truncated', out['truncated'])
print('pools', pools)
print('breakdown', out['breakdown'])
print('acquire', out['acquire'])
for g in out['queryGroups'][:15]:
    print(g['id'], g['count'], g['total'], g['mean'], g['p95'], g['max'], g['sql'][:220])
