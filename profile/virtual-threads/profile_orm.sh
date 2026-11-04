#!/bin/bash

# The recordings of VIRTUAL_THREADS_PROFILE.md: the ORM with platform threads and with a virtual thread for each session,
# 2 cores and 8 connections, at 15000 sessions/s without a delay and at 1500 sessions/s with a 1 ms delay.
# For each one, 4 fixed rate runs: without a recording (with perf stat), async-profiler cpu, async-profiler wall,
# and the JDK's Flight Recorder (with perf stat), each one for the measurement window only.
# Needs PostgreSQL (start_pg.sh, as run_OrmVsReactive.sh starts it), the orm-vs-reactive build, async-profiler and perf.
#
# Environment variables:
#   OUT          Output directory (default: /tmp/virtual-threads)
#   ASPROF_HOME  async-profiler directory (default: /opt/async-profiler)
#   JAVA_HOME    JDK (default: /usr/lib/jvm/java-25-openjdk)
#   EXTRA_OPTS   More JVM options, e.g. -Djdk.pollerMode=1

cd "$(dirname "$0")/../.."
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-25-openjdk}
ASPROF_HOME=${ASPROF_HOME:-/opt/async-profiler}
BIN=orm-vs-reactive/target/install/hibernate-orm-benchmark-orm-vs-reactive/bin/hibernate-orm-benchmark-orm-vs-reactive
OUT=${OUT:-/tmp/virtual-threads}
WARMUP=15
DURATION=20
mkdir -p "$OUT"
# PostgreSQL's cpu.stat, like run_OrmVsReactive.sh
cid=$(podman inspect -f '{{.Id}}' postgres-bench)
PG=$(find /sys/fs/cgroup -name "libpod-$cid.scope" -print -quit 2> /dev/null)/cpu.stat

# One fixed rate run of the ORM with one kind of recording during the measurement window
# usage: prof <tag> <cpus> <none|cpu|wall|jfr> <options of the benchmark...>
prof() {
	local tag=$1 cpus=$2 kind=$3
	shift 3
	local last=$(( 8 + cpus - 1 ))
	local jopts="-Xms1g -Xmx1g ${EXTRA_OPTS}"
	if [ "$kind" == "jfr" ]; then
		jopts="$jopts -XX:StartFlightRecording=delay=$(( WARMUP + 1 ))s,duration=$(( DURATION - 2 ))s,filename=$OUT/$tag.jfr,settings=profile"
		jopts="$jopts,jdk.VirtualThreadPinned#threshold=0ms,jdk.VirtualThreadSubmitFailed#enabled=true,jdk.SocketRead#threshold=0ms,jdk.ThreadPark#threshold=0ms"
	fi
	JAVA_OPTS="$jopts" taskset -c "8-$last" "$BIN" "$@" mode=fixed warmupSeconds=$WARMUP durationSeconds=$DURATION ${PG:+pgCpuStat=$PG} > "$OUT/$tag.log" 2>&1 &
	local launcher=$!
	until grep -q '^Warming up' "$OUT/$tag.log" 2> /dev/null; do
		kill -0 $launcher 2> /dev/null || { echo "The benchmark ended early, see $OUT/$tag.log"; return 1; }
		sleep 0.1
	done
	sleep $(( WARMUP + 1 ))
	# The launcher script execs java
	local pid=$launcher
	case "$kind" in
		cpu) "$ASPROF_HOME/bin/asprof" start -e cpu -i 200us -o jfr -f "$OUT/$tag.jfr" $pid > /dev/null ;;
		wall) "$ASPROF_HOME/bin/asprof" start -e wall -i 1ms -t -o jfr -f "$OUT/$tag.jfr" $pid > /dev/null ;;
	esac
	if [ "$kind" == "jfr" ] || [ "$kind" == "none" ]; then
		perf stat -e task-clock,context-switches,cpu-migrations -p $pid -o "$OUT/$tag.perf" -- sleep $(( DURATION - 2 )) > /dev/null 2>&1
	else
		sleep $(( DURATION - 2 ))
		"$ASPROF_HOME/bin/asprof" stop $pid > /dev/null
	fi
	wait $launcher
	grep '^RESULT' "$OUT/$tag.log" | sed "s/^RESULT/RESULT tag=$tag/" >> "$OUT/results.txt"
	echo "$tag done"
}

for regime in "c2-p8-l0-r15000:poolSize=8 dbLatencyMs=0 rate=15000" "c2-p8-l1-r1500:poolSize=8 dbLatencyMs=1 rate=1500"; do
	name=${regime%%:*}
	options=${regime#*:}
	for executor in "platform:engine=orm executor=platform threads=8" "virtual:engine=orm executor=virtual"; do
		for kind in none cpu wall jfr; do
			prof "$name-${executor%%:*}-$kind" 2 $kind ${executor#*:} $options
		done
	done
done
