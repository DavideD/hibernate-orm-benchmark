#!/bin/bash

# Compare Vert.x 4.5, Vert.x 5 with the Netty it brings, and Vert.x 5 with another Netty release
# (see NETTY_SELECT_ROUNDING.md, section 9): the Vert.x only reproducer, the benchmark,
# and the steal policies of POOL_STEALING.md section 11. Needs PostgreSQL (start_pg.sh) and the installs
# v4, vertx5 and vertx5n (built with -Pstack=vertx5 -PnettyVersion=...) as install-* and repro-* in $INSTALL_DIR.
#
# Environment variables: INSTALL_DIR, OUT (see profile_run.sh), REPS (default 2)

REPS=${REPS:-2}
export EVENT=none
cd "$(dirname "$0")"

for (( rep = 1; rep <= REPS; rep++ )); do
	stacks="v4 vertx5 vertx5n"
	(( rep % 2 == 0 )) && stacks="vertx5n vertx5 v4"
	for stack in $stacks; do
		for pool in perEventLoop shared; do
			for cache in false true; do
				./run_repro.sh $stack repro-$stack-$pool-cache$cache-r$rep rate=4000 poolType=$pool cachePreparedStatements=$cache > /dev/null \
					|| echo "Failed: repro $stack $pool $cache"
			done
		done
	done
done

for (( rep = 1; rep <= REPS; rep++ )); do
	stacks="v4 vertx5 vertx5n"
	(( rep % 2 == 0 )) && stacks="vertx5n vertx5 v4"
	for stack in $stacks; do
		for pool in perEventLoop shared; do
			for cache in false true; do
				./profile_run.sh $stack hr-$stack-$pool-cache$cache-r$rep rate=4000 poolType=$pool poolSize=4 poolEventLoopSize=4 eventLoops=4 cachePreparedStatements=$cache > /dev/null \
					|| echo "Failed: hr $stack $pool $cache"
			done
		done
	done
done

for stack in vertx5n vertx5 v4; do
	for rate in 2000 4000; do
		for pool in shared perEventLoop always alwaysActive preferLocalWait never; do
			./profile_run.sh $stack steal-$stack-$rate-$pool-r1 rate=$rate poolType=$pool poolSize=4 poolEventLoopSize=4 eventLoops=4 > /dev/null \
				|| echo "Failed: steal $stack $rate $pool"
		done
	done
done
