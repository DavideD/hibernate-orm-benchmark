import sys,re,collections
# usage: incl.py file sessions interval_us pattern...
f=sys.argv[1]; sess=float(sys.argv[2]); iv=float(sys.argv[3]); pats=sys.argv[4:]
tot=0; c=collections.Counter()
for line in open(f):
    stack,n=line.rstrip().rsplit(' ',1); n=int(n); tot+=n
    for p in pats:
        if re.search(p,stack): c[p]+=n
print(f'{f}: total {tot} samples = {tot*iv/sess:.1f} us/session')
for p in pats:
    print(f'  {c[p]:7d} {100*c[p]/tot:6.2f}% {c[p]*iv/sess:7.2f} us/sess  {p}')
