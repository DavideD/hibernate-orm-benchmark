#!/bin/bash

# Run the Vert.x only reproducer on Vert.x 4.5.35 (v4), 5.1.10 (vertx5), 5.2.2 (vertx522), and 5.1.10 with the one line
# Netty patch of VERTX5_CPU_PROFILE.md (vertx5p, install made by hand), same load for all.
# Then profile the timer case on v4 and vertx5, and run the case without Vert.x timers.
#
# Environment variables: REPS (default 2), RATE (default 4000), OUT (default /tmp/profile/out)

REPS=${REPS:-2}
RATE=${RATE:-4000}
export OUT=${OUT:-/tmp/profile/out}
cd "$(dirname "$0")"

for (( rep = 1; rep <= REPS; rep++ )); do
	for pool in perEventLoop shared; do
		for cache in false true; do
			for stack in v4 vertx5 vertx5p vertx522; do
				[ -d /tmp/profile/repro-$stack ] || continue
				EVENT=none ./run_repro.sh $stack "repro-$stack-$pool-cache$cache-timer-r$rep" rate=$RATE poolType=$pool cachePreparedStatements=$cache
			done
		done
	done
	for stack in v4 vertx5 vertx5p; do
		EVENT=none ./run_repro.sh $stack "repro-$stack-perEventLoop-cachefalse-thread-r$rep" rate=$RATE poolType=perEventLoop generator=thread
		EVENT=cpu-clock ./run_repro.sh $stack "reprocpu-$stack-perEventLoop-cachefalse-timer-r$rep" rate=$RATE poolType=perEventLoop
	done
done
