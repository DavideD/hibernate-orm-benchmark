import sys,re,collections
# classify.py sessions interval_us file...
CATS=[
 ('thr:GC',            r'^\[(GC Thread|G1 )'),
 ('thr:JIT',           r'^\[(C1|C2) Compiler'),
 ('thr:load-scheduler',r'^\[load-scheduler'),
 ('thr:MasterPoller',  r'^\[MasterPoller'),
 ('thr:FJP delayScheduler', r'^\[ForkJoinPool-1-delayScheduler'),
 ('thr:other',         r'^\[(?!request-|ForkJoinPool-1-worker)'),
 ('VT poller (subPollerLoop)', r'subPollerLoop|Poller\.pollerLoop'),
 ('Agroal getConnection/return', r'io\.agroal\.pool\.ConnectionPool\.(getConnection|returnConnection|handlerFromSharedCache|internalGetConnection)|ConnectionHandler\.transferConnection'),
 ('socket write0 (send+loopback rx softirq)', r'SocketDispatcher\.write0'),
 ('socket read0', r'SocketDispatcher\.read0'),
 ('platform blocking poll() (Net.poll)', r'Net\.poll'),
 ('VT poller register/deregister (epoll_ctl)', r'Poller\.(register|deregister|poll)\b|Poller\.poll_|EPoll\.ctl|Poller\.register'),
 ('VT park/yield/freeze', r'VirtualThread\.(park|yieldContinuation|tryYield|afterYield)|Continuation\.(yield|doYield)|freeze'),
 ('VT unpark/submit/signalWork', r'VirtualThread\.(unpark|submitRunContinuation|lazySubmit|externalSubmit)|ForkJoinPool\.(signalWork|execute|poolSubmit)|Unsafe\.unpark|LockSupport\.unpark'),
 ('FJP worker loop idle/scan (awaitWork, deactivate, park)', r'ForkJoinPool\.(awaitWork|deactivate|scan)|ForkJoinPool\.runWorker_\[j\]$|ForkJoinPool\.runWorker_\[j\];(?!java\.util\.concurrent\.ForkJoinPool\$WorkQueue\.topLevelExec)|nextLocalTask|topLevelExec_\[j\]$|ForkJoinTask\.doExec_\[i\]$'),
 ('TPE getTask/LBQ (platform hand-off)', r'ThreadPoolExecutor\.getTask|LinkedBlockingQueue'),
 ('session: Hibernate+Agroal+pgjdbc Java (+JVM runtime below it)', r'OrmEngine\.run_|org\.hibernate|org\.postgresql|io\.agroal'),
 ('VT mount/thaw/runContinuation', r'cont_thaw|cont_returnBarrier|thaw|Thaw|enterSpecial|VirtualThread\.(run|runContinuation|mount|unmount)|Continuation\.run'),
 ('worker: other', r'.'),
]
CR=[(n,re.compile(p)) for n,p in CATS]
sess=float(sys.argv[1]); iv=float(sys.argv[2]); files=sys.argv[3:]
res=[]
for f in files:
    c=collections.Counter(); tot=0
    for line in open(f):
        stack,n=line.rstrip().rsplit(' ',1); n=int(n); tot+=n
        for name,r in CR:
            if r.search(stack): c[name]+=n; break
    res.append((f,c,tot))
print('%-60s'%'category', ''.join('%22s'%f.split('/')[-1].replace('-cpu.txt','')[-20:] for f,_,_ in res))
for name,_ in CATS:
    print('%-60s'%name[:60], ''.join('%10.2f us %7.1f%%'%(c[name]*iv/sess, 100*c[name]/tot) for f,c,tot in res))
print('%-60s'%'TOTAL', ''.join('%10.2f us %7.1f%%'%(tot*iv/sess,100) for f,c,tot in res))
