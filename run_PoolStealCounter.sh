#!/bin/bash

# Start PostgreSQL, build the JMH jar and run ReactivePoolStealBenchmark,
# printing how many connections are stolen from other event loops.
#
# Any argument is passed to JMH and replaces the matching default below, e.g.:
#   ./run_PoolStealCounter.sh -t 16 -p poolSize=2 -p eventLoopCount=2 -p poolEventLoopSize=0,2
#
# Environment variables:
#   JAVA_HOME     JDK used for Gradle and JMH (default: a JDK 21 in /usr/lib/jvm, Gradle 8.4 can't run on 25)
#   PODMAN        Container command (default: podman, or "sudo podman" if rootless podman doesn't work)
#   STACK         Versions to build with: vertx5 or 6.6 (default: ORM 7.4 + HR 3.4 + Vert.x 4.5)
#   SKIP_BUILD    Set to true to reuse the jar from a previous build
#   STOP_DB       Set to true to stop the database when the benchmark ends

set -e

# Same container as start_pg.sh
CONTAINER_NAME="postgres-bench"
JAR=reactive/target/libs/hibernate-orm-benchmark-reactive-1.0-SNAPSHOT-jmh.jar

# Default scenario: all requests on event loop 0, plain Vert.x pool vs Quarkus-like pool
DEFAULT_ARGS=(
	-f 1 -wi 1 -w 3s -i 1 -r 5s
	-p targetInterArrivalNs=0
	-t 8
	-p sleepMs=1
	-p eventLoopCount=4
	-p poolSize=8
	-p targetVerticle=0
	-p poolEventLoopSize=0,4
)

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
	sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
	exit 0
fi

cd "$(dirname "$0")"

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
if [ "$SKIP_BUILD" == "true" ] && [ -f "$JAR" ]; then
	echo "Skipping build, using $JAR"
else
	./gradlew :hibernate-orm-benchmark-reactive:jmhJar ${STACK:+-Pstack=$STACK}
fi

# --- Benchmark ---
echo ""
echo "=========================================="
echo "  ReactivePoolStealBenchmark.throughput"
echo "=========================================="
# JMH fails if an option appears twice: only keep the defaults not overridden on the command line
declare -A overridden
args=("$@")
for (( i = 0; i < ${#args[@]}; i++ )); do
	if [[ "${args[i]}" == "-p" ]]; then
		overridden["-p ${args[i+1]%%=*}"]=1
	elif [[ "${args[i]}" == -* ]]; then
		overridden["${args[i]}"]=1
	fi
done
jmh_args=()
for (( i = 0; i < ${#DEFAULT_ARGS[@]}; i += 2 )); do
	option=${DEFAULT_ARGS[i]}
	value=${DEFAULT_ARGS[i+1]}
	key=$option
	[[ "$option" == "-p" ]] && key="-p ${value%%=*}"
	[[ -z "${overridden[$key]}" ]] && jmh_args+=("$option" "$value")
done

echo "JMH options: ${jmh_args[*]} $*"
"$JAVA_HOME/bin/java" -jar "$JAR" ReactivePoolStealBenchmark.throughput "${jmh_args[@]}" "$@"
