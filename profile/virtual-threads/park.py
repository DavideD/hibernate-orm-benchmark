import sys,re,collections
def dur_ms(s):
    v,u=s.split()[:2]; v=float(v.replace(',',''))
    return v*{'ns':1e-6,'us':1e-3,'ms':1,'s':1e3,'min':6e4,'h':3.6e6}[u]
f=sys.argv[1]; sess=float(sys.argv[2])
ev=[]; cur=None
for line in open(f):
    l=line.strip()
    if l.startswith('jdk.ThreadPark {'): cur={'fr':[]}
    elif cur is None: continue
    elif l.startswith('startTime ='): cur['t']=l.split('=')[1].split('(')[0].strip()
    elif l.startswith('duration ='): cur['d']=dur_ms(l.split('=',1)[1])
    elif l.startswith('timeout ='): cur['to']=l.split('=',1)[1].strip()
    elif l.startswith('eventThread ='): cur['th']=re.sub(r'-\d+"','-N"',l.split('=',1)[1].split('(')[0].strip())
    elif l.startswith('parkedClass ='): cur['pc']=l.split('=',1)[1].split('(')[0].strip()
    elif re.match(r'^[a-zA-Z].*\(.*\)',l) and 'stackTrace' not in l: cur['fr'].append(re.sub(r'\(.*','',l))
    elif l=='}' : ev.append(cur); cur=None
g=collections.defaultdict(list)
for e in ev:
    site=' < '.join(x.split('.')[-2]+'.'+x.split('.')[-1] for x in e['fr'][1:3])
    g[(e.get('th'),site,e.get('to')!='N/A')].append(e['d'])
print(f,len(ev),'events', f'{len(ev)/sess:.2f}/session')
print(f'{"count":>8} {"/sess":>6} {"sum s":>8} {"p50 ms":>8} {"p90":>8} {"p99":>8} {"max":>8}  thread | site | timed')
for k,v in sorted(g.items(),key=lambda kv:-len(kv[1]))[:15]:
    v.sort(); n=len(v); q=lambda p:v[min(n-1,int(p*n))]
    print(f'{n:8d} {n/sess:6.3f} {sum(v)/1e3:8.2f} {q(.5):8.3f} {q(.9):8.3f} {q(.99):8.3f} {v[-1]:8.2f}  {k[0]} | {k[1]} | {k[2]}')
