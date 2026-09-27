#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 /path/to/pixels-checkout-with-ingest-contract" >&2
    exit 2
fi
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
pixels=$(cd -- "$1" && pwd)
build=$(mktemp -d)
trap 'rm -rf -- "$build"' EXIT
"${MVN:-mvn}" -B -ntp -f "$pixels/pixels-common/pom.xml" \
    org.apache.maven.plugins:maven-dependency-plugin:2.10:build-classpath \
    -DincludeArtifactIds=protobuf-java -Dmdep.outputFile="$build/dependencies.cp" \
    > "$build/dependencies.log" 2>&1 || { cat "$build/dependencies.log" >&2; exit 1; }
dependencies=$(tr -d '\n' < "$build/dependencies.cp")
common="$pixels/pixels-common/src/main/java/io/pixelsdb/pixels/common/ingest"
retina="$pixels/pixels-retina/src/main/java/io/pixelsdb/pixels/retina/ingest"
writer="$repo/connector/src/main/java/io/pixelsdb/pixels/trino/write"
tests="$repo/connector/src/test/java/io/pixelsdb/pixels/trino/write"
# Does not compile the Trino SPI, protobuf adapters, native code, or Maven reactor.
javac --release 8 -cp "$dependencies" -Xlint:all -Xlint:-options -Werror -d "$build" \
    "$common"/*.java "$retina/LocalMutationJournal.java" \
    "$writer/PixelsMutationWriter.java" "$tests/MutationWriterContract.java" \
    "$repo/tools/ingest-contract/WriterJournalIntegration.java"
java -cp "$build:$dependencies" io.pixelsdb.pixels.trino.write.MutationWriterContract
java -cp "$build:$dependencies" WriterJournalIntegration
