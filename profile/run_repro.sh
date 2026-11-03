#!/bin/bash

# Run the Vert.x only reproducer (VertxCpuRepro) on both Vert.x versions with the same options, pinned like profile_run.sh.
# Needs the two installs: copy vertx-repro/target/install/* (built with and without -Pstack=vertx5)
# to $INSTALL_DIR/repro-v4 and $INSTALL_DIR/repro-vertx5. Output: OUT/<tag>.{log,jfr}
#
# Usage: run_repro.sh <stack> <tag> [options of VertxCpuRepro...]    Environment: EVENT (none or cpu-clock), see profile_run.sh

set -e
stack=$1
tag=$2
shift 2
INSTALL_DIR=${INSTALL_DIR:-/tmp/profile}
ASPROF_HOME=${ASPROF_HOME:-/tmp/profile/ap}
OUT=${OUT:-/tmp/profile/out}
EVENT=${EVENT:-none}
CPUS=${CPUS:-8-15}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-25-openjdk}
export JAVA_OPTS="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -Xms512m -Xmx512m"
duration=20
for arg in "$@"; do
	[[ $arg == durationSeconds=* ]] && duration=${arg#durationSeconds=}
done

mkdir -p "$OUT"
taskset -c "$CPUS" "$INSTALL_DIR/repro-$stack/bin/hibernate-orm-benchmark-vertx-repro" "$@" > "$OUT/$tag.log" 2>&1 &
pid=$!
until grep -q '^Measuring for' "$OUT/$tag.log" 2> /dev/null; do
	kill -0 $pid 2> /dev/null || { echo "The reproducer ended early, see $OUT/$tag.log"; exit 1; }
	sleep 0.1
done
if [ "$EVENT" != "none" ]; then
	"$ASPROF_HOME/bin/asprof" start -e "$EVENT" -i "${INTERVAL:-200us}" -o jfr -f "$OUT/$tag.jfr" $pid > /dev/null
	sleep $(( duration - 1 ))
	"$ASPROF_HOME/bin/asprof" stop $pid > /dev/null
fi
wait $pid
grep '^RESULT' "$OUT/$tag.log"
