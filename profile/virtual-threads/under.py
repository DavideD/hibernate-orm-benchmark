import sys,re,collections
# usage: under.py file anchor-regex depth [limit]: what is called below the anchor frame (next `depth` frames), self incl
f=sys.argv[1]; a=re.compile(sys.argv[2]); d=int(sys.argv[3]); lim=int(sys.argv[4]) if len(sys.argv)>4 else 30
c=collections.Counter(); tot=0
for line in open(f):
    stack,n=line.rstrip().rsplit(' ',1); n=int(n)
    fr=stack.split(';')
    idx=[i for i,x in enumerate(fr) if a.search(x)]
    if not idx: continue
    i=idx[-1]; tot+=n
    c[';'.join(fr[i+1:i+1+d]) or '(self)']+=n
print(f,'anchor',sys.argv[2],tot)
for k,v in c.most_common(lim): print(f'{v:7d} {100*v/tot:6.2f}% {k[-300:]}')
