#!/bin/bash

# Run OpenLoopStealBenchmark once, with async-profiler attached for the measurement window only.
# The warm-up is not recorded: the profiler starts when the benchmark prints "Measuring for"
# and stops when the window ends.
#
# Usage: profile_run.sh <stack> <tag> [benchmark options...]
#   stack   v4 (Hibernate Reactive 3.4.6 + Vert.x 4.5.35) or vertx5 (Hibernate Reactive 4.5.11 + Vert.x 5.1.10)
#   tag     name of the run, the output is OUT/<tag>.{jfr,log}
#
# Environment variables:
#   INSTALL_DIR  Where the two installDist outputs are, as install-v4 and install-vertx5 (default: /tmp/profile)
#   ASPROF_HOME  async-profiler directory (default: /tmp/profile/ap)
#   OUT          Output directory (default: /tmp/profile/out)
#   EVENT        cpu-clock (default), ctimer, itimer, alloc, or none to run without the profiler
#   INTERVAL     Sampling interval (default: 200us, ignored for alloc, which uses 512k)
#   CPUS         CPUs of the JVM (default: 8-15)
#   JAVA_HOME    JDK (default: /usr/lib/jvm/java-25-openjdk)
#
# Build the two installs first:
#   gradle :hibernate-orm-benchmark-reactive-openloop:installDist                 ; cp -r reactive-openloop/target/install/* $INSTALL_DIR/install-v4
#   gradle :hibernate-orm-benchmark-reactive-openloop:installDist -Pstack=vertx5  ; cp -r reactive-openloop/target/install/* $INSTALL_DIR/install-vertx5

set -e

stack=$1
tag=$2
shift 2

INSTALL_DIR=${INSTALL_DIR:-/tmp/profile}
ASPROF_HOME=${ASPROF_HOME:-/tmp/profile/ap}
OUT=${OUT:-/tmp/profile/out}
EVENT=${EVENT:-cpu-clock}
INTERVAL=${INTERVAL:-200us}
CPUS=${CPUS:-8-15}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-25-openjdk}

bin=$INSTALL_DIR/install-$stack/bin/hibernate-orm-benchmark-reactive-openloop
mkdir -p "$OUT"
log=$OUT/$tag.log
jfr=$OUT/$tag.jfr
duration=20
for arg in "$@"; do
	[[ $arg == durationSeconds=* ]] && duration=${arg#durationSeconds=}
done

# DebugNonSafepoints: the frames of inlined methods are accurate
export JAVA_OPTS="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -Xms1g -Xmx1g"
taskset -c "$CPUS" "$bin" "$@" > "$log" 2>&1 &
launcher=$!

# The launcher script execs java, so $launcher is the JVM
until grep -q '^Measuring for' "$log" 2> /dev/null; do
	kill -0 $launcher 2> /dev/null || { echo "The benchmark ended early, see $log"; exit 1; }
	sleep 0.1
done

if [ "$EVENT" != "none" ]; then
	if [ "$EVENT" == "alloc" ]; then
		"$ASPROF_HOME/bin/asprof" start -e alloc --alloc 512k --total -o jfr -f "$jfr" $launcher > /dev/null
	else
		"$ASPROF_HOME/bin/asprof" start -e "$EVENT" -i "$INTERVAL" -o jfr -f "$jfr" $launcher > /dev/null
	fi
	# Stop just before the benchmark takes its final snapshot
	sleep $(( duration - 1 ))
	"$ASPROF_HOME/bin/asprof" stop $launcher > /dev/null
fi
wait $launcher
grep '^RESULT' "$log" | tee "$OUT/$tag.result"
