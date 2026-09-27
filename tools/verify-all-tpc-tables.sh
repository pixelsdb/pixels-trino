#!/usr/bin/env bash
# Copy every TPC-H/TPC-DS tiny table into an already running Pixels catalog.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
JDBC_URL=${1:-jdbc:trino://127.0.0.1:8080}
DATA_ROOT=${2:?Usage: verify-all-tpc-tables.sh [jdbc-url] data-root [target-schema] [user]}
TARGET_SCHEMA=${3:-tpc_insert_all}
TRINO_USER=${4:-${USER:-pixels}}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/pixels-all-tpc-XXXXXXXX")
trap 'rm -rf "$WORK"' EXIT

if [[ $(javac -version 2>&1) != 'javac 23'* ]]; then
    echo "Trino 466 TPC verification requires JDK 23" >&2
    exit 2
fi

mvn -B -ntp -f "$ROOT/tools/ingest-contract/sql-runtime/pom.xml" \
    dependency:build-classpath -Dmdep.outputFile="$WORK/classpath" >/dev/null
mkdir -p "$WORK/classes"
javac -cp "$(tr -d '\n' < "$WORK/classpath")" -d "$WORK/classes" \
    "$ROOT/tools/ingest-contract/sql-runtime/src/main/java/io/pixelsdb/pixels/trino/testing/AllTpcTablesInsert.java"
java -cp "$WORK/classes:$(tr -d '\n' < "$WORK/classpath")" \
    io.pixelsdb.pixels.trino.testing.AllTpcTablesInsert \
    "$JDBC_URL" "$DATA_ROOT" "$TARGET_SCHEMA" "$TRINO_USER"
