import sys,re,collections
# Aggregate by deepest Java frame matching a package regex  (exclusive "owner")
f=sys.argv[1]; n_=int(sys.argv[2]) if len(sys.argv)>2 else 40
c=collections.Counter(); tot=0
for line in open(f):
    stack,n=line.rstrip().rsplit(' ',1); n=int(n); tot+=n
    fr=stack.split(';')[1:]
    # find last java frame ([j],[i],[0],[1]) 
    jl=[x for x in fr if re.search(r'_\[[ji01]\]$',x)]
    last=jl[-1] if jl else '(no java) '+(fr[0] if fr else '')
    c[last]+=n
print(f,tot)
for k,v in c.most_common(n_): print(f'{v:7d} {100*v/tot:6.2f}% {k}')
