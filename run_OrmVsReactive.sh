#!/bin/bash

# Start PostgreSQL, build the orm-vs-reactive module and run OrmVsReactiveBenchmark for each combination of
# CPU budget, pool size, database latency and configuration, then print a summary table.
#
# Each run starts the same requests (read-only sessions of N finds, after an optional pg_sleep) at random times
# (Poisson arrivals) on Hibernate ORM (blocking JDBC, Agroal) or Hibernate Reactive (Vert.x PostgreSQL client).
# The JVM is pinned to the first CPUs of a range with taskset, so that the CPU is the bottleneck, and
# PostgreSQL to other CPUs. MODE=max looks for the highest rate that each configuration sustains
# (at least 99% of the requests complete, p99 under SLO_MS, no errors), MODE=fixed runs at RATE.
#
# Any argument is passed to every run as an option, e.g.:
#   ./run_OrmVsReactive.sh queriesPerSession=10
#
# Options and defaults: queriesPerSession=5 sloMs=10 startRate=1000 rampFactor=1.5 precision=1.04 stepSeconds=5 preparedStatements=true
#                       settleSeconds=1 warmupSeconds=15 durationSeconds=20 seed=42 (see OrmVsReactiveBenchmark)
#
# Environment variables:
#   CONFIGS       What to run (default: "orm:platform orm:virtual reactive:shared reactive:localWait reactive:perEventLoop").
#                 orm:platform[:THREADS]  blocking sessions on a fixed pool of platform threads (default POOL_SIZE threads)
#                 orm:virtual             blocking sessions on virtual threads
#                 reactive:POOL[:epoll][:pipeN]  Hibernate Reactive with the pool shared, localWait or perEventLoop,
#                                         on NIO or epoll, with PgConnectOptions#setPipeliningLimit(N)
#   CPUS          CPU budgets of the JVM, in cores (default: "4"). The JVM gets the CPUs JVM_FIRST.. JVM_FIRST+N-1
#   POOL_SIZES    Connections of the pool (default: "8")
#   LATENCIES     Milliseconds that the database sleeps at the start of each session (default: "0")
#   MODE          max or fixed (default: max)
#   RATE          Requests per second for MODE=fixed (default: 2000)
#   REPEAT        Rounds (default: 1). The configurations alternate between rounds (the order is reversed every other round)
#   ROUND_OFFSET  Added to the round number in the results, to tell apart several calls of this script (default: 0)
#   JVM_FIRST     First CPU of the JVM (default: 8, the physical cores of the second half of a 16-core CPU)
#   PG_CPUSET     CPUs of PostgreSQL (default: 16-23); PG_CPUS is its CPU quota (default: 8),
#                 PG_MAX_CONNECTIONS its max_connections (default: 400)
#   OUT           File where the RESULT lines are appended (default: results.txt in a new directory in /tmp)
#   JAVA_HOME     JDK used for Gradle and the benchmark (default: a JDK 21 or 25 in /usr/lib/jvm, the wrapper's
#                 Gradle 8.4 can't run on 25: use a newer Gradle with GRADLE=gradle)
#   GRADLE        Gradle command (default: ./gradlew)
#   JAVA_OPTS     JVM options of the benchmark (default: -Xms1g -Xmx1g)
#   PODMAN        Container command (default: podman, or "sudo podman" if rootless podman doesn't work)
#   EPOLL         Netty version of the native epoll transport for the :epoll configurations (default: 4.2.19.Final)
#   SKIP_BUILD    Set to true to reuse the previous build
#   STOP_DB       Set to true to stop the database when the benchmark ends

set -e

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
	sed -n '3,42p' "$0" | sed 's/^# \{0,1\}//'
	exit 0
fi

cd "$(dirname "$0")"

CONFIGS=${CONFIGS:-"orm:platform orm:virtual reactive:shared reactive:localWait reactive:perEventLoop"}
CPUS=${CPUS:-4}
POOL_SIZES=${POOL_SIZES:-8}
LATENCIES=${LATENCIES:-0}
MODE=${MODE:-max}
RATE=${RATE:-2000}
REPEAT=${REPEAT:-1}
ROUND_OFFSET=${ROUND_OFFSET:-0}
JVM_FIRST=${JVM_FIRST:-8}
PG_CPUSET=${PG_CPUSET:-16-23}
PG_CPUS=${PG_CPUS:-8}
PG_MAX_CONNECTIONS=${PG_MAX_CONNECTIONS:-400}
EPOLL=${EPOLL:-4.2.19.Final}
GRADLE=${GRADLE:-./gradlew}
export JAVA_OPTS=${JAVA_OPTS:-"-Xms1g -Xmx1g"}
OUT=${OUT:-"$(mktemp -d /tmp/orm-vs-reactive.XXXXXX)/results.txt"}
# Same container as start_pg.sh
CONTAINER_NAME="postgres-bench"
BIN=orm-vs-reactive/target/install/hibernate-orm-benchmark-orm-vs-reactive/bin/hibernate-orm-benchmark-orm-vs-reactive

# --- JDK ---
if [ -z "$JAVA_HOME" ]; then
	for jdk in /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/java-21 /usr/lib/jvm/java-25-openjdk /usr/lib/jvm/java-25; do
		if [ -x "$jdk/bin/java" ]; then
			JAVA_HOME=$jdk
			break
		fi
	done
fi
if [ -z "$JAVA_HOME" ]; then
	echo "ERROR: no JDK 21 or 25 found, set JAVA_HOME"
	exit 1
fi
export JAVA_HOME
echo "Using JAVA_HOME=$JAVA_HOME, results in $OUT"

# --- Database ---
if [ -z "$PODMAN" ]; then
	if podman info > /dev/null 2>&1; then
		PODMAN="podman"
	else
		PODMAN="sudo podman"
	fi
fi

if [ "$($PODMAN inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null)" == "true" ]; then
	echo "Reusing running container $CONTAINER_NAME (check that it is pinned to the CPUs $PG_CPUSET)"
else
	# Host networking: the port forwarding of a rootless container takes CPU outside of the pinned CPUs
	PODMAN="$PODMAN" CPUS="$PG_CPUS" CPUSET="$PG_CPUSET" MAX_CONNECTIONS="$PG_MAX_CONNECTIONS" ./start_pg.sh -n
fi

if [ "$STOP_DB" == "true" ]; then
	trap '$PODMAN stop "$CONTAINER_NAME" > /dev/null && echo "Stopped $CONTAINER_NAME"' EXIT
fi

# The CPU time of PostgreSQL, to check that it isn't the bottleneck
container_id=$($PODMAN inspect -f '{{.Id}}' "$CONTAINER_NAME")
PG_CPU_STAT=$(find /sys/fs/cgroup -name "libpod-$container_id.scope" -print -quit 2> /dev/null)/cpu.stat
if [ ! -r "$PG_CPU_STAT" ]; then
	echo "PostgreSQL's cpu.stat not found: its CPU won't be reported"
	PG_CPU_STAT=
fi

# --- Build ---
if [ "$SKIP_BUILD" == "true" ] && [ -x "$BIN" ]; then
	echo "Skipping build, using $BIN"
else
	$GRADLE :hibernate-orm-benchmark-orm-vs-reactive:installDist -Pstack=vertx5 ${EPOLL:+-Pepoll=$EPOLL}
fi

# --- Benchmark ---
# The options of a configuration, from its name
config_options() {
	local config=$1 pool_size=$2
	local IFS=:
	set -- $config
	case "$1" in
		orm)
			case "$2" in
				platform) echo "engine=orm executor=platform threads=${3:-$pool_size}" ;;
				virtual) echo "engine=orm executor=virtual" ;;
				*) echo "ERROR: unknown ORM configuration $config" >&2; return 1 ;;
			esac
			;;
		reactive)
			local options="engine=reactive poolType=$2"
			shift 2
			for part in "$@"; do
				case "$part" in
					epoll) options="$options nativeTransport=true" ;;
					pipe*) options="$options pipeliningLimit=${part#pipe}" ;;
					*) echo "ERROR: unknown option $part in $config" >&2; return 1 ;;
				esac
			done
			echo "$options"
			;;
		*) echo "ERROR: unknown configuration $config" >&2; return 1 ;;
	esac
}

mkdir -p "$(dirname "$OUT")"
run_log=$(mktemp)
for (( round = 1; round <= REPEAT; round++ )); do
	configs=$CONFIGS
	if (( round % 2 == 0 )); then
		configs=$(echo $CONFIGS | tr ' ' '\n' | tac | tr '\n' ' ')
	fi
	for cpus in $CPUS; do
		last=$(( JVM_FIRST + cpus - 1 ))
		for pool_size in $POOL_SIZES; do
			for latency in $LATENCIES; do
				for config in $configs; do
					options=$(config_options "$config" "$pool_size")
					echo ""
					echo "=========================================="
					echo "  cpus=$cpus poolSize=$pool_size dbLatencyMs=$latency $config mode=$MODE $* (round $round/$REPEAT)"
					echo "=========================================="
					# Not tee /dev/stderr: when the output goes to a file, tee would reopen it and truncate it
					taskset -c "$JVM_FIRST-$last" "$BIN" $options "mode=$MODE" "rate=$RATE" "poolSize=$pool_size" "dbLatencyMs=$latency" \
							${PG_CPU_STAT:+pgCpuStat=$PG_CPU_STAT} "$@" 2>&1 | tee "$run_log"
					# The configuration and the run, to tell the runs apart in the summary
					grep '^RESULT' "$run_log" | sed "s/^RESULT/RESULT config=$config cpus=$cpus round=$(( round + ROUND_OFFSET ))/" >> "$OUT" || true
				done
			done
		done
	done
done 2>&1
rm -f "$run_log"

# --- Summary ---
echo ""
echo "MODE=max: maxRate is the highest rate that was sustained, in requests/s; the other columns are the ones of that step."
echo "CPU us is the CPU time of the whole process per request, busy us the time the CPUs of the JVM were busy per request"
echo "(it includes the kernel's network stack and other processes), DB cores the CPU of PostgreSQL."
awk -v mode="$MODE" '
	{
		delete r
		for ( i = 2; i <= NF; i++ ) { split( $i, kv, "=" ); r[kv[1]] = kv[2] }
		key = r["cpus"] "|" r["poolSize"] "|" r["dbLatencyMs"] "|" r["config"]
		if ( !( key in count ) ) { order[++n] = key }
		count[key]++
		rate = ( mode == "max" ) ? r["maxRate"] : r["achievedRate"]
		if ( !( key in min ) || rate + 0 < min[key] ) min[key] = rate + 0
		if ( !( key in max ) || rate + 0 > max[key] ) max[key] = rate + 0
		cpu[key] += r["cpuUsPerReq"]; busy[key] += r["busyUsPerReq"]; p99[key] += r["p99Us"]; threads[key] = r["peakThreads"]; db[key] += r["pgCpuCores"]
	}
	END {
		for ( i = 1; i <= n; i++ ) {
			key = order[i]; split( key, k, "|" )
			printf "%4s %5s %7s %-28s %4d %11s %8.1f %8.1f %9.0f %8s %8.2f\n", k[1], k[2], k[3], k[4], count[key],
				( min[key] == max[key] ? min[key] : min[key] "-" max[key] ), cpu[key] / count[key], busy[key] / count[key], p99[key] / count[key], threads[key], db[key] / count[key]
		}
	}
' "$OUT" | {
	printf "%4s %5s %7s %-28s %4s %11s %8s %8s %9s %8s %8s\n" cores pool "lat ms" config runs "req/s" "CPU us" "busy us" "p99 us" threads "DB cores"
	cat
}
