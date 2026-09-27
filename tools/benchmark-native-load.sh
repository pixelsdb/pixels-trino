#!/usr/bin/env bash
# Compare verified TPC projection data with the existing Pixels CLI consumer.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PIXELS=$(cd "${1:?Usage: benchmark-native-load.sh pixels sql-evidence tpch-or-tpcds fresh-work}" && pwd)
EVIDENCE=$(cd "${2:?SQL evidence directory required}" && pwd)
DATASET=${3:?Dataset required}
WORK=${4:?Fresh work directory required}
: "${PIXELS_HOME:?Matching Pixels runtime required}"
NATIVE_LOAD_THREADS=${NATIVE_LOAD_THREADS:-4}
NATIVE_LOAD_FILE_ROWS=${NATIVE_LOAD_FILE_ROWS:-4000000}
NATIVE_LOAD_HEAP=${NATIVE_LOAD_HEAP:-6g}
[[ ! -e "$WORK" ]] || { echo "Use a fresh work directory" >&2; exit 2; }
[[ -f "$EVIDENCE/exit-code" && $(<"$EVIDENCE/exit-code") == 0 ]] || {
    echo "Source SQL run must have completed successfully" >&2; exit 2;
}
[[ "$DATASET" == tpch || "$DATASET" == tpcds ]] || exit 2
[[ "$NATIVE_LOAD_THREADS" =~ ^[1-9][0-9]*$ && "$NATIVE_LOAD_FILE_ROWS" =~ ^[1-9][0-9]*$ ]] || exit 2
[[ "$NATIVE_LOAD_HEAP" =~ ^[1-9][0-9]*[mMgG]$ ]] || exit 2
[[ -f "$PIXELS/pixels-cli/target/classes/io/pixelsdb/pixels/cli/load/SimplePixelsConsumer.class" ]] || {
    echo "Build pixels-cli with the Pixels build JDK first" >&2; exit 2;
}
mkdir -p "$WORK/classes"
WORK=$(cd "$WORK" && pwd)
mvn -B -ntp -f "$PIXELS/pixels-cli/pom.xml" \
    org.apache.maven.plugins:maven-dependency-plugin:2.10:build-classpath \
    -Dmdep.includeScope=runtime -Dmdep.outputFile="$WORK/cli.cp" > "$WORK/dependencies.log" 2>&1
CLASSPATH="$(<"$EVIDENCE/backend.cp"):$PIXELS/pixels-cli/target/classes:$(<"$WORK/cli.cp")"
javac -cp "$CLASSPATH" -d "$WORK/classes" \
    "$ROOT/tools/ingest-contract/native-runtime/NativeLoadBenchmark.java"
SOURCE_ROOT=$(sed -n 's/^dataRoot=//p' "$EVIDENCE/status.properties")
java -XX:ActiveProcessorCount=4 "-Xmx$NATIVE_LOAD_HEAP" \
    --enable-native-access=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$WORK/classes:$CLASSPATH" \
    io.pixelsdb.pixels.daemon.transaction.ingest.NativeLoadBenchmark \
    "$SOURCE_ROOT/ordered" "$WORK/data" "$DATASET" \
    "$NATIVE_LOAD_THREADS" "$NATIVE_LOAD_FILE_ROWS" 2>&1 | tee "$WORK/load.log"
grep -q '^NATIVE_LOAD_PASS ' "$WORK/load.log"
