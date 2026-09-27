#!/usr/bin/env bash
# Real SQL -> connector -> TCP RPC -> Retina -> SQLite MainIndex/Pixels files.
set -euo pipefail
TRINO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PIXELS=$(cd "${1:?Usage: verify-sql-insert.sh /path/to/pixels [Maven arguments...]}" && pwd)
shift
MVN=${MVN:-mvn}
MAVEN_ARGS=("$@")
: "${PIXELS_HOME:?PIXELS_HOME must point to a built Pixels runtime with native Retina libraries}"
WORK=${SQL_E2E_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pixels-sql-e2e-XXXXXXXX")}
SQL_E2E_MAIN_CLASS=${SQL_E2E_MAIN_CLASS:-io.pixelsdb.pixels.trino.testing.FullSqlInsert}
SQL_E2E_TIMEOUT_SECONDS=${SQL_E2E_TIMEOUT_SECONDS:-180}
SQL_E2E_BACKEND_HEAP=${SQL_E2E_BACKEND_HEAP:-1g}
SQL_E2E_ENGINE_HEAP=${SQL_E2E_ENGINE_HEAP:-3g}
SQL_E2E_BACKEND_STOP_TIMEOUT_SECONDS=${SQL_E2E_BACKEND_STOP_TIMEOUT_SECONDS:-60}
SQL_E2E_VISIBILITY_BARRIER_TIMEOUT_SECONDS=${SQL_E2E_VISIBILITY_BARRIER_TIMEOUT_SECONDS:-30}
SQL_E2E_RUN_FAILURE_SCENARIOS=${SQL_E2E_RUN_FAILURE_SCENARIOS:-true}
SQL_E2E_PASS_PATTERN=${SQL_E2E_PASS_PATTERN:-'(^|[[:space:]])FULL_SQL_INSERT_E2E_PASS checks=[0-9]+ rows=1008 trinoWorkers=2( |$)'}
if [[ ! "$SQL_E2E_MAIN_CLASS" =~ ^[A-Za-z_][A-Za-z0-9_.]*$ ]] ||
   [[ ! "$SQL_E2E_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
   [[ ! "$SQL_E2E_BACKEND_STOP_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
   [[ ! "$SQL_E2E_VISIBILITY_BARRIER_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
   [[ ! "$SQL_E2E_RUN_FAILURE_SCENARIOS" =~ ^(true|false)$ ]] ||
   [[ ! "$SQL_E2E_BACKEND_HEAP" =~ ^[1-9][0-9]*[mMgG]$ ]] ||
   [[ ! "$SQL_E2E_ENGINE_HEAP" =~ ^[1-9][0-9]*[mMgG]$ ]]; then
    echo "Invalid SQL E2E main class, timeout, stop timeout, barrier timeout, failure mode, or backend heap" >&2
    exit 2
fi
SQL_E2E_MAIN_SOURCE="$TRINO/tools/ingest-contract/sql-runtime/src/main/java/${SQL_E2E_MAIN_CLASS//./\/}.java"
[[ -f "$SQL_E2E_MAIN_SOURCE" ]] || { echo "Missing SQL driver source: $SQL_E2E_MAIN_SOURCE" >&2; exit 2; }
mkdir -p "$WORK"
WORK=$(cd "$WORK" && pwd)
if [[ -e "$WORK/ready" || -e "$WORK/stop" ]]; then
    echo "Use a fresh SQL_E2E_WORK_DIR to avoid stale fixture state" >&2
    exit 2
fi
if [[ $(javac -version 2>&1) != 'javac 23'* ]]; then
    echo "Trino 466 SQL verification requires JDK 23" >&2
    exit 2
fi

if [[ ${SQL_E2E_SKIP_BUILD:-0} != 1 ]]; then
    "$MVN" -B -f "$PIXELS/pom.xml" "${MAVEN_ARGS[@]}" -DskipTests=true install > "$WORK/pixels-build.log" 2>&1
    "$MVN" -B -f "$TRINO/pom.xml" "${MAVEN_ARGS[@]}" -DskipTests=true test-compile > "$WORK/trino-build.log" 2>&1
fi
DEPENDENCIES=org.apache.maven.plugins:maven-dependency-plugin:2.10:build-classpath
"$MVN" -B -f "$TRINO/tools/ingest-contract/sql-runtime/pom.xml" "${MAVEN_ARGS[@]}" "$DEPENDENCIES" \
    -Dmdep.outputFile="$WORK/engine.cp" > "$WORK/engine-dependencies.log" 2>&1
"$MVN" -B -f "$TRINO/connector/pom.xml" "${MAVEN_ARGS[@]}" "$DEPENDENCIES" -Dmdep.includeScope=runtime \
    -Dmdep.outputFile="$WORK/plugin-dependencies.cp" > "$WORK/plugin-dependencies.log" 2>&1
"$MVN" -B -f "$PIXELS/pixels-daemon/pom.xml" "${MAVEN_ARGS[@]}" "$DEPENDENCIES" \
    -Dmdep.outputFile="$WORK/backend-dependencies.cp" > "$WORK/backend-dependencies.log" 2>&1

# Use a deployment-style plugin directory with normal filename ordering. Keep the
# full runtime dependency set, including Spike; do not expose it to engine Jersey.
mkdir -p "$WORK/plugin" "$WORK/driver-classes"
jar --create --file "$WORK/plugin/pixels-trino-connector.jar" -C "$TRINO/connector/target/classes" .
python3 - "$WORK" "$PIXELS" <<'PY'
import os
import sys
from pathlib import Path
work, pixels = map(Path, sys.argv[1:])
for item in (work / 'plugin-dependencies.cp').read_text().strip().split(os.pathsep):
    source = Path(item).resolve(strict=True)
    link = work / 'plugin' / source.name
    if link.exists() and link.resolve() != source:
        raise RuntimeError('Duplicate plugin artifact filename: ' + source.name)
    if not link.exists():
        link.symlink_to(source)
(work / 'plugin.cp').write_text(os.pathsep.join(str(p) for p in sorted((work / 'plugin').glob('*.jar'))))
backend = [pixels / 'pixels-daemon/target/test-classes', pixels / 'pixels-daemon/target/classes']
(work / 'backend.cp').write_text(os.pathsep.join(map(str, backend)) + os.pathsep + (work / 'backend-dependencies.cp').read_text().strip())
PY
javac -cp "$(cat "$WORK/engine.cp")" -d "$WORK/driver-classes" \
    -sourcepath "$TRINO/tools/ingest-contract/sql-runtime/src/main/java" \
    "$SQL_E2E_MAIN_SOURCE"

export LD_LIBRARY_PATH="$PIXELS_HOME/lib:${LD_LIBRARY_PATH:-}"
# Jemalloc-enabled Retina uses a process-wide allocator. Scope interposition to
# the backend JVM, rather than injecting it into Trino and shell helper tools.
BACKEND_ENV=(env)
if [[ -f "$PIXELS_HOME/lib/libjemalloc.so.2" ]]; then
    BACKEND_ENV+=("LD_PRELOAD=$PIXELS_HOME/lib/libjemalloc.so.2${LD_PRELOAD:+:$LD_PRELOAD}")
fi
JAVA_ARGS=(-XX:ActiveProcessorCount=4 --enable-native-access=ALL-UNNAMED
    "-Dpixels.sql.visibility-barrier-timeout-seconds=$SQL_E2E_VISIBILITY_BARRIER_TIMEOUT_SECONDS"
    "-Dpixels.sql.run-failure-scenarios=$SQL_E2E_RUN_FAILURE_SCENARIOS"
    "-XX:ErrorFile=$WORK/hs_err_pid%p.log"
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED)
BACKEND_PID=''
cleanup() {
    local result=$?
    trap - EXIT
    touch "$WORK/stop"
    if [[ -n "$BACKEND_PID" ]]; then
        backend_stop_deadline=$((SECONDS + SQL_E2E_BACKEND_STOP_TIMEOUT_SECONDS))
        while kill -0 "$BACKEND_PID" 2>/dev/null && ((SECONDS < backend_stop_deadline)); do
            sleep .1
        done
        if kill -0 "$BACKEND_PID" 2>/dev/null; then
            echo "Backend did not stop after fixture cleanup" >&2
            [[ "$result" != 0 ]] || result=1
            kill "$BACKEND_PID" 2>/dev/null || true
            sleep .1
            kill -KILL "$BACKEND_PID" 2>/dev/null || true
        fi
        if wait "$BACKEND_PID"; then
            :
        else
            local backend_result=$?
            echo "Backend exited with status $backend_result" >&2
            [[ "$result" != 0 ]] || result=$backend_result
        fi
    fi
    if [[ "$result" != 0 ]]; then
        tail -80 "$WORK/sql.log" 2>/dev/null || true
        tail -30 "$WORK/backend.log" 2>/dev/null || true
        tail -30 "$WORK/runtime.log" 2>/dev/null || true
    fi
    printf '%s\n' "$result" > "$WORK/exit-code"
    printf 'SQL verification exit=%s evidence=%s\n' "$result" "$WORK"
    exit "$result"
}
trap cleanup EXIT
terminate_on_signal() {
    local status=$1
    trap - INT TERM
    exit "$status"
}
trap 'terminate_on_signal 130' INT
trap 'terminate_on_signal 143' TERM

# Capture failures that occur before the backend reaches its Java main method.
"${BACKEND_ENV[@]}" java "${JAVA_ARGS[@]}" -version > "$WORK/runtime.log" 2>&1
{
    printf '\nRetina native dependencies:\n'
    ldd "$PIXELS_HOME/lib/libpixels-retina.so"
} >> "$WORK/runtime.log" 2>&1

# The backend is a separate process with its own dependency graph. Only the
# catalog, node directory and external ID source are fixtures; writes are real.
"${BACKEND_ENV[@]}" java "${JAVA_ARGS[@]}" "-Xmx$SQL_E2E_BACKEND_HEAP" -cp "$(cat "$WORK/backend.cp")" \
    io.pixelsdb.pixels.daemon.transaction.ingest.SqlIngestFixtureMain "$WORK" > "$WORK/backend.log" 2>&1 &
BACKEND_PID=$!
for _ in $(seq 1 300); do
    [[ -f "$WORK/ready" ]] && break
    kill -0 "$BACKEND_PID" 2>/dev/null || { echo "Backend exited before readiness" >&2; exit 1; }
    sleep .1
done
[[ -f "$WORK/ready" ]] || { echo "Backend readiness deadline exceeded" >&2; exit 1; }
export PIXELS_CONFIG="$WORK/pixels.properties"
timeout -k 10s "${SQL_E2E_TIMEOUT_SECONDS}s" java "${JAVA_ARGS[@]}" "-Xmx$SQL_E2E_ENGINE_HEAP" \
    -cp "$WORK/driver-classes:$(cat "$WORK/engine.cp")" \
    "$SQL_E2E_MAIN_CLASS" "$WORK" "$WORK/plugin.cp" > "$WORK/sql.log" 2>&1
cat "$WORK/sql.log"
grep -Eq "$SQL_E2E_PASS_PATTERN" "$WORK/sql.log"
