import sys,re,collections
for f in sys.argv[1:]:
    c=collections.Counter(); tot=0
    for line in open(f):
        stack,n=line.rstrip().rsplit(' ',1); n=int(n); tot+=n
        leaf=stack.split(';')[-1]
        if leaf.endswith('_[k]'): k='kernel'
        elif re.search(r'_\[[ji01]\]$',leaf): k='java'
        else: k='native/vm'
        c[k]+=n
    print(f,tot,{k:f'{100*v/tot:.1f}%' for k,v in c.items()})
