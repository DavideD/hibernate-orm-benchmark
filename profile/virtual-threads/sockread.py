import sys,re
def dur_ms(s):
    v,u=s.split()[:2]; v=float(v.replace(',',''))
    return v*{'ns':1e-6,'us':1e-3,'ms':1,'s':1e3,'min':6e4,'h':3.6e6}[u]
for f in sys.argv[1:]:
    d=[];th={}
    cur=None
    for line in open(f):
        l=line.strip()
        if l.startswith('duration ='): cur=dur_ms(l.split('=',1)[1])
        elif l.startswith('eventThread =') and cur is not None:
            t=re.sub(r'-\d+"','-N"',l.split('=',1)[1].split('(')[0].strip()); th[t]=th.get(t,0)+1; d.append(cur); cur=None
    d.sort(); n=len(d); q=lambda p:d[min(n-1,int(p*n))]
    print(f.split('/')[-1], n, th, 'mean %.3f p10 %.3f p50 %.3f p90 %.3f p99 %.3f p99.9 %.3f max %.2f ms'%(sum(d)/n,q(.1),q(.5),q(.9),q(.99),q(.999),d[-1]))
