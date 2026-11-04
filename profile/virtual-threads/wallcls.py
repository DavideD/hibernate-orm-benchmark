import sys,re,collections
CATS=[
 ('idle: TPE getTask/LBQ.take park', r'ThreadPoolExecutor\.getTask'),
 ('idle: FJP awaitWork park', r'ForkJoinPool\.awaitWork'),
 ('FJP scan/deactivate (spinning)', r'ForkJoinPool\.(deactivate|scan)|ForkJoinPool\.runWorker_\[j\]$|nextLocalTask'),
 ('VT poller (subPollerLoop)', r'subPollerLoop'),
 ('session: Agroal getConnection', r'io\.agroal\.pool\.ConnectionPool\.getConnection'),
 ('session: wait DB reply in poll()', r'Net\.poll'),
 ('session: write0', r'SocketDispatcher\.write0'),
 ('session: read0', r'SocketDispatcher\.read0'),
 ('session: epoll_ctl register', r'Poller\.(register|deregister|poll)_|EPoll\.ctl'),
 ('session: VT park/yield', r'VirtualThread\.(park|yieldContinuation)|Continuation\.yield'),
 ('session: VT unpark', r'VirtualThread\.unpark|Unsafe\.unpark|signalWork'),
 ('session: other Java (Hibernate/pgjdbc)', r'OrmEngine\.run|org\.hibernate|org\.postgresql|io\.agroal'),
 ('VT mount/thaw', r'cont_|thaw|Thaw|runContinuation|enterSpecial'),
 ('other', r'.'),
]
CR=[(n,re.compile(p)) for n,p in CATS]
thr=re.compile(sys.argv[1])
for f in sys.argv[2:]:
    c=collections.Counter(); tot=0
    for line in open(f):
        stack,n=line.rstrip().rsplit(' ',1); n=int(n)
        if not thr.search(stack.split(';',1)[0]): continue
        tot+=n
        for name,r in CR:
            if r.search(stack): c[name]+=n; break
    print(f, tot, 'samples')
    for name,_ in CATS:
        if c[name]: print(f'  {c[name]:7d} {100*c[name]/tot:6.2f}%  {name}')
