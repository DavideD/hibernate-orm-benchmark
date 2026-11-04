import sys,re,collections
for f in sys.argv[1:]:
    c=collections.Counter(); tot=0
    for line in open(f):
        stack,n=line.rstrip().rsplit(' ',1); n=int(n)
        th=stack.split(';',1)[0]
        th=re.sub(r' tid=\d+','',th); th=re.sub(r'request-\d+','request-N',th); th=re.sub(r'worker-\d+','worker-N',th)
        c[th]+=n; tot+=n
    print('==',f,tot)
    for k,v in c.most_common(25): print(f'  {v:8d} {100*v/tot:6.2f}% {k}')
