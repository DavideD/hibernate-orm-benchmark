import sys,re,collections
# usage: depth.py file depth [filter-regex] : top-down aggregate at a given depth (after thread frame)
f=sys.argv[1]; d=int(sys.argv[2]); flt=sys.argv[3] if len(sys.argv)>3 else None
c=collections.Counter(); tot=0
for line in open(f):
    stack,n=line.rstrip().rsplit(' ',1); n=int(n)
    if flt and not re.search(flt,stack): continue
    fr=stack.split(';'); th=re.sub(r' tid=\d+|-\d+\b','',fr[0])
    tot+=n; c[(th,';'.join(x.split('(')[0] for x in fr[1:1+d]))]+=n
print(f,tot)
for k,v in c.most_common(int(sys.argv[4]) if len(sys.argv)>4 else 40): print(f'{v:7d} {100*v/tot:6.2f}% {k[0]} {k[1][-400:]}')
