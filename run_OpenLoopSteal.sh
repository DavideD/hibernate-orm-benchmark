#!/bin/bash

# Start PostgreSQL, build the reactive-openloop module and run OpenLoopStealBenchmark
# for each request rate and pool type, then print a summary table.
#
# Each run uses a fixed request rate, below the maximum throughput, and measures the CPU used by the event loops,
# how often they go to sleep (each sleep needs a wake-up), and the latency.
# Compare poolType=shared (stealing) with poolType=perEventLoop (no stealing) at the same rate.
#
# Any argument is passed to every run as an option, e.g.:
#   ./run_OpenLoopSteal.sh poolSize=8 weights=4,2,1,1 queriesPerSession=10
#
# Options and defaults: eventLoops=4 poolSize=16 poolEventLoopSize=4 weights=1,1,1,1 queriesPerSession=5
#                       warmupSeconds=10 durationSeconds=20 seed=42
#                       localWaitMs=1 (preferLocalWait) localWaitUs=1000 localWaitDepth=0 creationAffinity=true (localWait)
#                       nativeTransport=false (true needs EPOLL)
#
# Environment variables:
#   RATES         Total requests per second to test (default: "1000 2000 4000")
#   POOL_TYPES    Pools to test (default: "shared perEventLoop"), also: releasesFirst, localWait,
#                 always, alwaysActive, preferLocalWait, never
#   REPEAT        Runs for each combination (default: 1)
#   JAVA_HOME     JDK used for Gradle and the benchmark (default: a JDK 21 in /usr/lib/jvm, Gradle 8.4 can't run on 25)
#   PODMAN        Container command (default: podman, or "sudo podman" if rootless podman doesn't work)
#   STACK         Versions to build with: vertx5 or 6.6 (default: ORM 7.4 + HR 3.4 + Vert.x 4.5)
#   EPOLL         Netty version of the native epoll transport to use instead of NIO, e.g. 4.2.19.Final (Vert.x 5)
#   SKIP_BUILD    Set to true to reuse the previous build
#   STOP_DB       Set to true to stop the database when the benchmark ends

set -e

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
	sed -n '3,28p' "$0" | sed 's/^# \{0,1\}//'
	exit 0
fi

cd "$(dirname "$0")"

RATES=${RATES:-"1000 2000 4000"}
POOL_TYPES=${POOL_TYPES:-"shared perEventLoop"}
REPEAT=${REPEAT:-1}
# Same container as start_pg.sh
CONTAINER_NAME="postgres-bench"
BIN=reactive-openloop/target/install/hibernate-orm-benchmark-reactive-openloop/bin/hibernate-orm-benchmark-reactive-openloop

# --- JDK ---
if [ -z "$JAVA_HOME" ]; then
	for jdk in /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/java-21 /usr/lib/jvm/java-17-openjdk; do
		if [ -x "$jdk/bin/java" ]; then
			JAVA_HOME=$jdk
			break
		fi
	done
fi
if [ -z "$JAVA_HOME" ]; then
	echo "ERROR: no JDK 17 or 21 found, set JAVA_HOME (Gradle 8.4 doesn't run on newer JDKs)"
	exit 1
fi
export JAVA_HOME
echo "Using JAVA_HOME=$JAVA_HOME"

# --- Database ---
if [ -z "$PODMAN" ]; then
	if podman info > /dev/null 2>&1; then
		PODMAN="podman"
	else
		PODMAN="sudo podman"
	fi
fi

if [ "$($PODMAN inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null)" == "true" ]; then
	echo "Reusing running container $CONTAINER_NAME"
else
	PODMAN="$PODMAN" ./start_pg.sh
fi

if [ "$STOP_DB" == "true" ]; then
	trap '$PODMAN stop "$CONTAINER_NAME" > /dev/null && echo "Stopped $CONTAINER_NAME"' EXIT
fi

# --- Build ---
if [ "$SKIP_BUILD" == "true" ] && [ -x "$BIN" ]; then
	echo "Skipping build, using $BIN"
else
	./gradlew :hibernate-orm-benchmark-reactive-openloop:installDist ${STACK:+-Pstack=$STACK} ${EPOLL:+-Pepoll=$EPOLL}
fi

# --- Benchmark ---
results=$(mktemp)
run_log=$(mktemp)
for rate in $RATES; do
	for poolType in $POOL_TYPES; do
		for (( i = 1; i <= REPEAT; i++ )); do
			echo ""
			echo "=========================================="
			echo "  rate=$rate poolType=$poolType $* (run $i/$REPEAT)"
			echo "=========================================="
			# Not tee /dev/stderr: when the output goes to a file, tee would reopen it and truncate it
			"$BIN" "rate=$rate" "poolType=$poolType" "$@" 2>&1 | tee "$run_log"
			grep '^RESULT' "$run_log" >> "$results" || true
		done
	done
done 2>&1

# --- Summary ---
echo ""
echo "Event loop CPU and sleeps are per request; CPU% is the total for all the event loops,"
echo "max loop the CPU of the busiest one (100% = one core)."
awk '
	{
		delete r
		for ( i = 2; i <= NF; i++ ) { split( $i, kv, "=" ); r[kv[1]] = kv[2] }
		printf "%6s %-16s %8s %7s%% %7s%% %8s%% %8s %8s %7s %7s %7s %7s\n", r["rate"], r["poolType"], r["achievedRate"], r["stolenPct"],
			r["loopCpuPct"], r["busiestLoopCpuPct"], r["loopCpuUsPerReq"], r["loopSleepsPerReq"], r["p50Us"], r["p99Us"], r["p999Us"], r["failed"]
	}
' "$results" | sort -n -k1,1 -s | {
	printf "%6s %-16s %8s %8s %8s %9s %8s %8s %7s %7s %7s %7s\n" rate poolType "req/s" stolen "CPU%" "max loop" "CPU us" sleeps "p50 us" "p99 us" "p99.9" failed
	cat
}

# With probe=true: the acquisitions that missed a connection of their event loop
if grep -q 'missedPct=' "$results"; then
	echo ""
	echo "no local: acquisitions that found no free connection on their event loop."
	echo "missed: of those, the ones where the release of a connection of their event loop was in the queue of the pool."
	echo "to local release: time until the pool processed the release of a connection of their event loop."
	awk '
		/missedPct=/ {
			delete r
			for ( i = 2; i <= NF; i++ ) { split( $i, kv, "=" ); r[kv[1]] = kv[2] }
			printf "%6s %-16s %8s%% %8s%% %8s%% %10s %10s %10s\n", r["rate"], r["poolType"], r["stolenPct"], r["noLocalFreePct"],
				r["missedPct"], r["toLocalReleaseP50Us"], r["toLocalReleaseP90Us"], r["toLocalReleaseP99Us"]
		}
	' "$results" | sort -n -k1,1 -s | {
		printf "%6s %-16s %9s %9s %9s %10s %10s %10s\n" rate poolType stolen "no local" missed "p50 us" "p90 us" "p99 us"
		cat
	}
fi
rm -f "$results" "$run_log"
