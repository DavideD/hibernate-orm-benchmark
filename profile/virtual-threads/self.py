import sys,re,collections
f=sys.argv[1]; lim=int(sys.argv[2]); flt=re.compile(sys.argv[3]) if len(sys.argv)>3 else None
c=collections.Counter(); tot=0
for line in open(f):
    stack,n=line.rstrip().rsplit(' ',1); n=int(n)
    if flt and not flt.search(stack): continue
    tot+=n; c[stack.split(';')[-1]]+=n
print(f,tot)
for k,v in c.most_common(lim): print(f'{v:7d} {100*v/tot:6.2f}% {k}')
