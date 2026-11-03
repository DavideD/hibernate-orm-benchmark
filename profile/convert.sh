#!/bin/bash

# Convert the recordings of run_matrix.sh to collapsed stacks (one per recording, event loop threads and the others),
# a flame graph per recording and a differential flame graph, Vert.x 5 minus Vert.x 4.5, per pool type and cache setting.
#
# Usage: convert.sh [out dir] [converted dir]

OUT=${1:-/tmp/profile/out}
DEST=${2:-/tmp/profile/col}
ASPROF_HOME=${ASPROF_HOME:-/tmp/profile/ap}
mkdir -p "$DEST"

for jfr in "$OUT"/cpu-*.jfr; do
	name=$(basename "$jfr" .jfr)
	"$ASPROF_HOME/bin/jfrconv" --cpu --norm --dot -t -o collapsed "$jfr" "$DEST/$name.collapsed" > /dev/null
	"$ASPROF_HOME/bin/jfrconv" --cpu --norm --dot -o html --title "$name" "$jfr" "$DEST/$name.html" > /dev/null
done
for jfr in "$OUT"/alloc-*.jfr; do
	name=$(basename "$jfr" .jfr)
	"$ASPROF_HOME/bin/jfrconv" --alloc --total --norm --dot -t -o collapsed "$jfr" "$DEST/$name.collapsed" > /dev/null
	"$ASPROF_HOME/bin/jfrconv" --alloc --total --norm --dot -o html --title "$name" "$jfr" "$DEST/$name.html" > /dev/null
done

# Differential flame graphs of the first repetition: the Vert.x 4.5 recording first, then Vert.x 5
for pool in shared perEventLoop; do
	for cache in false true; do
		"$ASPROF_HOME/bin/jfrconv" --cpu --norm --dot --diff -o html --title "Vert.x 5 minus 4.5, $pool, cache $cache" \
			"$OUT/cpu-v4-$pool-cache$cache-r1.jfr" "$OUT/cpu-vertx5-$pool-cache$cache-r1.jfr" "$DEST/diff-$pool-cache$cache.html" > /dev/null
	done
done
