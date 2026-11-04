import sys,re,collections
# events per bucket by thread from jfr print text
f=sys.argv[1]; b=int(sys.argv[2])  # bucket ms
c=collections.defaultdict(collections.Counter); t=None
for line in open(f):
    l=line.strip()
    if l.startswith('startTime ='):
        hh,mm,ss=l.split('=')[1].split('(')[0].strip().split(':'); t=int((int(hh)*3600+int(mm)*60+float(ss))*1000)
    elif l.startswith('eventThread =') and t is not None:
        th=re.sub(r'-\d+"','-N"',l.split('=',1)[1].split('(')[0].strip()); c[t//b][th]+=1; t=None
ks=sorted(c); t0=ks[0]; ths=sorted({th for k in ks for th in c[k]})
print('t(ms)',*[x[:25] for x in ths],sep='\t')
for k in range(ks[0],ks[-1]+1):
    print((k-t0)*b,*[c[k][th] for th in ths],sep='\t')
