#!/usr/bin/env bash
# Benchmark independent small INSERT transactions through the public Trino JDBC interface.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
JDBC_URL=${1:-jdbc:trino://127.0.0.1:8080}
TARGET_SCHEMA=${2:-small_insert_benchmark}
TARGET_TABLE=${3:-events}
TRINO_USER=${4:-${USER:-pixels}}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/pixels-small-insert-XXXXXXXX")
trap 'rm -rf "$WORK"' EXIT

if [[ $(javac -version 2>&1) != 'javac 23'* ]]; then
    echo "Trino 466 JDBC benchmarks require JDK 23" >&2
    exit 2
fi

mvn -B -ntp -f "$ROOT/tools/ingest-contract/sql-runtime/pom.xml" \
    dependency:build-classpath -Dmdep.outputFile="$WORK/classpath" >/dev/null
mkdir -p "$WORK/classes"
javac -cp "$(tr -d '\n' < "$WORK/classpath")" -d "$WORK/classes" \
    "$ROOT/tools/ingest-contract/sql-runtime/src/main/java/io/pixelsdb/pixels/trino/testing/SmallTransactionsInsert.java"
java -cp "$WORK/classes:$(tr -d '\n' < "$WORK/classpath")" \
    io.pixelsdb.pixels.trino.testing.SmallTransactionsInsert \
    "$JDBC_URL" "$TARGET_SCHEMA" "$TARGET_TABLE" "$TRINO_USER"
