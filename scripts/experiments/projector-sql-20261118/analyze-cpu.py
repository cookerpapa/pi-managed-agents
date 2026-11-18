import json, sys, pathlib, collections
root = pathlib.Path(sys.argv[1])
r = json.load(open(root / 'cpu.json'))
p = r['profile']
m = json.load(open(root / 'load/report.json'))['measurements'][0]
a = m['startedAt']
b = a + m['summary']['elapsedMs']
nodes = {n['id']: n for n in p['nodes']}
parents = {c: n['id'] for n in p['nodes'] for c in n.get('children', [])}
time = p['startTime']
origin = r['startedAt'] - time / 1000
selfs = collections.Counter()
inclusive = collections.Counter()
modules = collections.Counter()
total = 0
for n, delta in zip(p['samples'], p['timeDeltas']):
    time += delta
    at = origin + time / 1000
    if not a <= at <= b:
        continue
    total += delta
    selfs[n] += delta
    chain = []
    seen = set()
    cur = n
    while cur:
        node = nodes[cur]
        f = node['callFrame']
        key = (f.get('functionName'), f.get('url'))
        if key not in seen:
            inclusive[key] += delta
            seen.add(key)
        chain.append(f)
        cur = parents.get(cur)
    f = nodes[n]['callFrame']
    modules[f.get('url') or f.get('functionName')] += delta
out = {'sampledMs': total / 1000, 'self': [{'milliseconds': v / 1000, **nodes[n]['callFrame']} for n, v in selfs.most_common()], 'inclusive': [{'milliseconds': v / 1000, 'functionName': k[0], 'url': k[1]} for k, v in inclusive.most_common()]}
(root / 'cpu-analysis.json').write_text(json.dumps(out, indent=2) + '\n')
print('sampledMs', total / 1000)
for n, v in selfs.most_common(25):
    print(round(v / 1000), round(v / total * 100, 2), nodes[n]['callFrame'])
print('MODULES')
for k, v in modules.most_common(18):
    print(round(v / 1000), round(v / total * 100, 2), k)
print('INCLUSIVE')
for k, v in inclusive.most_common(20):
    print(round(v / 1000), round(v / total * 100, 2), k)
