import sys,re,collections
# callers.py file leaf-regex depth lim : callers of leaf frames matching
f=sys.argv[1]; a=re.compile(sys.argv[2]); d=int(sys.argv[3]); lim=int(sys.argv[4])
c=collections.Counter(); tot=0
for line in open(f):
    stack,n=line.rstrip().rsplit(' ',1); n=int(n)
    fr=stack.split(';')
    if not a.search(fr[-1]): continue
    tot+=n; c[';'.join(fr[-1-d:-1])]+=n
print(f,tot)
for k,v in c.most_common(lim): print(f'{v:7d} {100*v/tot:6.2f}% {k[-350:]}')
