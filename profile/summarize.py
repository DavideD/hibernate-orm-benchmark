#!/usr/bin/env python3
"""Average the RESULT line of the logs in a directory, per tag without the -rN suffix, as a markdown table.

usage: summarize.py <out dir> <tag prefix> [<tag prefix> ...]     e.g. summarize.py /tmp/profile/out none- repro-
"""
import collections, glob, os, re, sys

out, prefixes = sys.argv[1], sys.argv[2:]
groups = collections.defaultdict(list)
for path in sorted(glob.glob(os.path.join(out, '*.log'))):
    tag = os.path.basename(path)[:-len('.log')]
    if not any(tag.startswith(p) for p in prefixes):
        continue
    lines = [l for l in open(path) if l.startswith('RESULT')]
    if not lines:
        continue
    line = lines[0].strip()
    kv = dict(x.split('=', 1) for x in line.split()[1:])
    groups[re.sub(r'-r\d+$', '', tag)].append(kv)

def avg(runs, key):
    values = [float(r[key]) for r in runs if key in r]
    return sum(values) / len(values) if values else None

def cell(v, fmt):
    return '' if v is None else fmt % v

print('| Run | Runs | Stolen | CPU per request | Sleeps per request | p50 | p99 |')
print('|---|---|---|---|---|---|---|')
def us(v):
    return '' if v is None else '%.0f µs' % v
for tag, runs in groups.items():
    cpus = [float(r['loopCpuUsPerReq']) for r in runs]
    spread = f' ({min(cpus):.0f}–{max(cpus):.0f})' if len(cpus) > 1 else ''
    print(f"| {tag} | {len(runs)} | {cell(avg(runs, 'stolenPct'), '%.0f%%')} | {avg(runs, 'loopCpuUsPerReq'):.0f} µs{spread} "
          f"| {cell(avg(runs, 'loopSleepsPerReq'), '%.1f')} | {us(avg(runs, 'p50Us'))} | {us(avg(runs, 'p99Us'))} |")
