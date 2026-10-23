#!/bin/bash

# Count the calls of a few JDK methods on the event loop threads during the measurement window, with
# async-profiler's Java method profiling (interval 1 = every call is a sample). Prints the calls per request.
# Each method needs its own run, a recording has one event. Same options as run_matrix.sh.
#
# Usage: count_calls.sh [benchmark options...]       Environment: see profile_run.sh

cd "$(dirname "$0")"
export OUT=${OUT:-/tmp/profile/out}
opts=${*:-rate=4000 poolType=perEventLoop poolSize=4 poolEventLoopSize=4 eventLoops=4}
for stack in v4 vertx5; do
	for m in sun.nio.ch.EPollSelectorImpl.doSelect sun.nio.ch.SocketChannelImpl.write sun.nio.ch.SocketChannelImpl.read; do
		tag=calls-$stack-${m##*.ch.}
		EVENT=$m INTERVAL=1 ./profile_run.sh $stack "$tag" $opts > /dev/null
		/tmp/profile/ap/bin/jfrconv --cpu -t -o collapsed "$OUT/$tag.jfr" "$OUT/$tag.collapsed" > /dev/null
		completed=$(sed -n 's/^Requests: \([0-9]*\) completed.*/\1/p' "$OUT/$tag.log")
		calls=$(grep '^\[vert.x-eventloop' "$OUT/$tag.collapsed" | awk '{s += $NF} END {print s}')
		awk -v s=$stack -v m=$m -v c=${calls:-0} -v n=$completed 'BEGIN {printf "%s %s: %d calls, %.2f per request\n", s, m, c, c / n}'
	done
done
