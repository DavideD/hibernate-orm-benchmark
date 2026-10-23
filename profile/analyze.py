#!/usr/bin/env python3
"""Compare async-profiler recordings of two stacks, per completed request.

usage: analyze.py [--top N] [--interval-us 200] <labelA>=<glob of collapsed files> <labelB>=<glob> ...
The collapsed files come from `jfrconv --cpu --norm --dot -t -o collapsed x.jfr x.collapsed`
(or --alloc --total for allocations, then the unit is bytes and --interval-us must be 1).
The completed requests are read from x.log next to the .jfr: set --logs to the directory with the logs.
With two groups, also prints the difference B - A. Several files in a group are averaged.
"""
import argparse, collections, glob, os, re, sys

p = argparse.ArgumentParser()
p.add_argument('groups', nargs='+')
p.add_argument('--top', type=int, default=25)
p.add_argument('--interval-us', type=float, default=200.0)
p.add_argument('--logs', default='/tmp/profile/out')
p.add_argument('--thread', default='vert.x-eventloop-thread-')
args = p.parse_args()

SUFFIX = re.compile(r'_\[(.)\]$')

def package(frame):
    m = SUFFIX.search(frame)
    kind = m.group(1) if m else ''
    name = SUFFIX.sub('', frame)
    if kind == 'k':
        return 'kernel'
    if '.' not in name and kind not in ('j', 'i', '0'):
        return 'native (libc, JVM)'
    for prefix, group in (
            ('io.netty.', 'io.netty'), ('io.vertx.core.', 'io.vertx.core'),
            ('io.vertx.sqlclient.', 'io.vertx.sqlclient'), ('io.vertx.pgclient.', 'io.vertx.pgclient'),
            ('io.vertx.', 'io.vertx (other)'),
            ('org.hibernate.reactive.benchmark.', 'benchmark'), ('org.hibernate.reactive.', 'org.hibernate.reactive'),
            ('org.hibernate.', 'org.hibernate (ORM)'), ('io.smallrye.', 'io.smallrye.mutiny'),
            ('java.', 'JDK'), ('jdk.', 'JDK'), ('sun.', 'JDK'), ('javax.', 'JDK')):
        if name.startswith(prefix):
            return group
    return 'other java'

def method(frame):
    return SUFFIX.sub('', frame)

def completed(path):
    log = os.path.join(args.logs, os.path.basename(path).replace('.collapsed', '.log'))
    for line in open(log):
        m = re.match(r'Requests: (\d+) completed', line)
        if m:
            return int(m.group(1))
    raise SystemExit('no request count in ' + log)

PHASES = (
    ('selector poll (select, selectNow)', ('NioEventLoop.select', 'NioIoHandler.select')),
    ('I/O: read, decode, callbacks', ('processSelectedKeys',)),
    ('tasks (runAllTasks)', ('runAllTasks',)),
)

def phase(frames):
    names = [method(f) for f in frames]
    for label, markers in PHASES:
        if any(n.endswith(m) for n in names for m in markers):
            return label
    return 'event loop overhead, other'

class Profile:
    """Samples per request, averaged over the files of a group."""
    def __init__(self, files):
        self.self_ = collections.Counter()
        self.total = collections.Counter()
        self.pkg = collections.Counter()
        self.pkg_total = collections.Counter()   # stacks that contain a frame of the package
        self.loop = 0.0
        self.other_threads = 0.0
        self.phase = collections.Counter()
        self.io_pkg = collections.Counter()   # self time by package inside the I/O phase
        for f in files:
            n = completed(f)
            w = args.interval_us / n / len(files)   # us (or bytes) per request
            for line in open(f):
                stack, count = line.rstrip('\n').rsplit(' ', 1)
                count = int(count)
                frames = stack.split(';')
                if not frames[0].startswith('[' + args.thread):
                    self.other_threads += count * w
                    continue
                frames = frames[1:]
                self.loop += count * w
                leaf = frames[-1]
                self.self_[method(leaf)] += count * w
                self.pkg[package(leaf)] += count * w
                ph = phase(frames)
                self.phase[ph] += count * w
                if ph.startswith('I/O'):
                    self.io_pkg[package(leaf)] += count * w
                for m in {method(x) for x in frames}:
                    self.total[m] += count * w
                for g in {package(x) for x in frames}:
                    self.pkg_total[g] += count * w

def table(title, rows, header):
    print(f'\n### {title}\n')
    print('| ' + ' | '.join(header) + ' |')
    print('|' + '---|' * len(header))
    for r in rows:
        print('| ' + ' | '.join(r) + ' |')

profiles = {}
for spec in args.groups:
    label, pattern = spec.split('=', 1)
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit('no files for ' + pattern)
    profiles[label] = Profile(files)
    print(f'{label}: {len(files)} file(s): ' + ', '.join(os.path.basename(f) for f in files))

labels = list(profiles)
for label, pr in profiles.items():
    print(f'\n## {label}: event loop {pr.loop:.1f} per request, other threads {pr.other_threads:.1f}')
    table('Event loop phases', [[g, f'{v:.1f}', f'{100 * v / pr.loop:.1f}%'] for g, v in pr.phase.most_common()],
          ['Phase', 'per request', 'share'])
    table('Self time by package inside the I/O phase', [[g, f'{v:.1f}'] for g, v in pr.io_pkg.most_common()],
          ['Package', 'per request'])
    table('Self time by package of the leaf frame', [[g, f'{v:.1f}', f'{100 * v / pr.loop:.1f}%'] for g, v in pr.pkg.most_common()],
          ['Package', 'per request', 'share'])
    table('Time under each package (inclusive)', [[g, f'{v:.1f}', f'{100 * v / pr.loop:.1f}%'] for g, v in pr.pkg_total.most_common()],
          ['Package', 'per request', 'share'])
    table('Top self time', [[m, f'{v:.2f}'] for m, v in pr.self_.most_common(args.top)], ['Method', 'per request'])
    table('Top total time', [[m, f'{v:.2f}'] for m, v in pr.total.most_common(args.top)], ['Method', 'per request'])

if len(labels) == 2:
    a, b = profiles[labels[0]], profiles[labels[1]]
    print(f'\n## Difference: {labels[1]} minus {labels[0]} (per request): {b.loop - a.loop:+.1f} of {a.loop:.1f}')
    for title, ca, cb in (('phase', a.phase, b.phase), ('package inside the I/O phase, self', a.io_pkg, b.io_pkg),
                          ('package, self', a.pkg, b.pkg), ('package, inclusive', a.pkg_total, b.pkg_total)):
        keys = set(ca) | set(cb)
        rows = sorted(((k, ca[k], cb[k]) for k in keys), key=lambda r: -abs(r[2] - r[1]))
        table('Difference by ' + title, [[k, f'{x:.1f}', f'{y:.1f}', f'{y - x:+.1f}'] for k, x, y in rows],
              ['Package', labels[0], labels[1], 'difference'])
    for title, ca, cb in (('self time', a.self_, b.self_), ('total time', a.total, b.total)):
        keys = set(ca) | set(cb)
        rows = sorted(((k, ca[k], cb[k]) for k in keys), key=lambda r: -(r[2] - r[1]))[:args.top * 2]
        table('Methods that grew most, ' + title, [[k, f'{x:.2f}', f'{y:.2f}', f'{y - x:+.2f}'] for k, x, y in rows],
              ['Method', labels[0], labels[1], 'difference'])
        rows = sorted(((k, ca[k], cb[k]) for k in keys), key=lambda r: (r[2] - r[1]))[:args.top]
        table('Methods that shrank most, ' + title, [[k, f'{x:.2f}', f'{y:.2f}', f'{y - x:+.2f}'] for k, x, y in rows],
              ['Method', labels[0], labels[1], 'difference'])
