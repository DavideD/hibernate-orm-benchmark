#!/bin/bash

# Profile both stacks under the same load: for each repetition, stack, pool type and prepared statement cache setting,
# one cpu-clock recording and one run without the profiler (for the CPU per request the benchmark itself reports),
# then one allocation recording per stack and pool type.
# Alternates the order of the stacks between repetitions. Needs PostgreSQL (start_pg.sh), see profile_run.sh.
#
# Environment variables: REPS (default 2), RATE (default 4000), OUT (default /tmp/profile/out)

REPS=${REPS:-2}
RATE=${RATE:-4000}
export OUT=${OUT:-/tmp/profile/out}
cd "$(dirname "$0")"

opts() { echo "rate=$RATE poolType=$1 poolSize=4 poolEventLoopSize=4 eventLoops=4 cachePreparedStatements=$2"; }

for (( rep = 1; rep <= REPS; rep++ )); do
	stacks="v4 vertx5"
	(( rep % 2 == 0 )) && stacks="vertx5 v4"
	for stack in $stacks; do
		for pool in shared perEventLoop; do
			for cache in false true; do
				tag=$stack-$pool-cache$cache-r$rep
				EVENT=cpu-clock ./profile_run.sh $stack "cpu-$tag" $(opts $pool $cache)
				EVENT=none ./profile_run.sh $stack "none-$tag" $(opts $pool $cache)
			done
		done
	done
done

for stack in v4 vertx5; do
	for pool in shared perEventLoop; do
		EVENT=alloc ./profile_run.sh $stack "alloc-$stack-$pool-cachefalse-r1" $(opts $pool false)
	done
done
