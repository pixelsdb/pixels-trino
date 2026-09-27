# Transactional INSERT: implementation, lifecycle and verification

Related: pixelsdb/pixels-trino#180 and pixelsdb/pixels-trino#179.

## Implemented scope

The connector supports autocommit and explicit multi-statement INSERT VALUES
and INSERT ... SELECT. One explicit transaction may write multiple tables in
the same Pixels catalog and publication domain, and each statement reads its
fixed public snapshot plus prior completed private writes. Trino Pages are encoded into bounded column batches,
routed through authenticated ingestion RPCs and staged in a transaction-private
Retina WAL. The durable coordinator records COMMIT or ABORT before installation.
VISIBLE commit acknowledgement waits until installed rows reach the published
read timestamp. DURABLE acknowledgement returns after the recoverable COMMIT
decision and installation continues in the backend. Applications can close the
current committed prefix and wait for visibility with:

~~~sql
CALL pixels.system.flush_visible_barrier(timeout_seconds => 30);
~~~

The barrier captures its committed boundary when called and does not wait for
later transactions or global idleness. `retina.ingest.file.max.delay.ms`
controls how long an underfilled FILE tail may remain open during normal
background aggregation. The barrier is different: it closes every tail needed
by the captured boundary immediately. Bulk loaders should therefore use
FILE+DURABLE, allow transactions to aggregate, and issue one barrier after the
last accepted transaction instead of one barrier per transaction.

BUFFERED installation reuses existing Retina write buffers. FILE installation
writes bounded row batches directly through PixelsWriter and never exposes a
public MemTable copy. Both representations reuse pixels-index:

- MainIndex: rowId -> RowLocation;
- supported primary and non-unique SinglePointIndex membership;
- existing row-id allocation.

A business primary key is optional. Keyless tables preserve duplicate rows.
Request/writer idempotency never deduplicates equal user values. Each concrete
PageSink obtains a durable transaction-local writer ID through AllocateWriter;
the Trino task ID remains trace metadata, not an ingestion stream identity.

This is a fixed-topology, single-owner, LOCAL durable implementation. It does
not provide replicated coordinator/WAL state, HA failover, task-attempt retry,
UPDATE/DELETE, or unique secondary constraints.

## Normal daemon lifecycle

When retina.ingest.enabled=true, normal TransServer and RetinaServer instances
register coordinator and participant services. The coordinator recovers its
locked state volume before accepting writes. Retina recovers the recovery
checkpoint, Storage-GC WAL, installation plans and mutation journal; it reports
READY only after participant recovery completes.
Coordinator-to-participant RPCs wait for a recovering participant within the
configured transaction deadline, so an early reconciliation connection failure
cannot leak as the first post-READY Prepare failure.

Startup fails closed for missing/corrupt confirmed state, a missing WAL
generation, inconsistent recovery coverage, a state-volume ownership conflict,
timestamp regression, invalid credentials, or unavailable required metadata.
A failed server cannot be restarted inside the same daemon process.

Shutdown first stops RPC admission, then closes the participant, background
GC/checkpoint work, shared write buffers and native visibility. Non-empty
buffers are materialized as Pixels files. Transaction WAL/plan data remains
unless its normal checkpoint handoff already made it reclaimable.

### Required configuration

Provision these settings while the feature remains disabled:

~~~properties
retina.enable=true
retina.ingest.enabled=false
retina.ingest.auth.secret.file=/absolute/secret/path
retina.ingest.coordinator.state.dir=/absolute/state/coordinator
retina.ingest.participant.plan.dir=/absolute/state/plans
retina.ingest.participant.wal.dir=/absolute/state/wal
retina.ingest.cutover.baseline.timestamp=0
retina.ingest.write.representation=BUFFERED
retina.ingest.commit.ack=VISIBLE
retina.ingest.file.target.rows=1000000
retina.ingest.file.max.bytes=536870912
retina.ingest.file.max.delay.ms=30000
retina.ingest.file.pixel.stride=10000
retina.ingest.coordinator.compaction.bytes=16777216
retina.ingest.max.transactions=10000
retina.ingest.terminal.retention.ms=86400000
retina.ingest.terminal.max.transactions=100000
retina.ingest.wal.segment.bytes=67108864
retina.ingest.wal.max.bytes=4294967296
retina.ingest.wal.max.records=10000000
retina.ingest.wal.group.commit.delay.micros=200
retina.ingest.wal.read.cache.max.bytes=67108864
retina.ingest.coordinator.group.commit.delay.micros=200
~~~

The three state directories must be absolute, pairwise non-overlapping,
persistent local volumes owned by exactly one live process. The credential must
contain 24–4096 visible ASCII bytes after trimming; deploy it with owner-only
permissions. RPC transport is plaintext, so use a trusted network or external
TLS boundary.

The WAL read cache is private to the Retina owner and bounded by bytes; it
avoids rereading recently appended batches during installation and private
reads. A sealed batch is served only after its WAL seal is durable. Evicted
batches and batches recovered after restart are read from the WAL.

The coordinator stores transaction changes in a checksummed incremental log
and periodically replaces it with an atomic checkpoint at
`retina.ingest.coordinator.compaction.bytes`. Confirmed corrupt records fail
startup; an incomplete trailing record is discarded during recovery.
The two bounded group-commit delays let concurrent seal and decision writers
share one local synchronization. A zero value disables the wait without
weakening the synchronized COMMIT boundary.

Start the coordinator role before the Retina role. Do not route INSERTs until
Retina reports:

~~~text
Retina service and transactional ingestion are ready
~~~

## Supported offline cutover

Cutover is operator controlled. Never apply it to a live production table
without a backup and rollback window.

1. Disable new Trino writes and drain all legacy CDC/LOAD/Retina writers for
   tables being switched. Stop old producers so no process retains a cached
   transaction-ID segment.
2. Wait for legacy write transactions to terminate and flush existing buffers.
   Verify catalog/files and indexes.
3. Run the existing Retina recovery-checkpoint cycle and record a fixed
   baseline B at least as high as both the legacy published timestamp and
   checkpoint applied timestamp. Pin old files and GC history through B.
4. With writers and transaction daemons stopped, advance the existing etcd
   trans_id value so the first new value is strictly greater than B. Do this as
   a guarded/CAS administrative change; never lower or reuse the key.
5. Set retina.ingest.cutover.baseline.timestamp=B and enable
   retina.ingest.enabled=true on coordinator and Retina. Start coordinator,
   then Retina. Startup rejects a baseline below the legacy publication domain
   or an allocator value at/below the recovered floor.
6. Verify old rows with a pinned snapshot and verify one new INSERT before
   reopening traffic. Keep the backup until a post-cutover checkpoint and
   GC-safe point complete.

With the gate enabled, legacy non-read-only transaction begins and the legacy
Retina updateRecord/streaming mutation RPC are rejected. Disable the gate only
for an explicit offline rollback after stopping new writers; never run both
write paths concurrently.

## Checkpoint and reclamation

COMMIT, PUBLISHED, file close and age are not reclamation conditions. The
durable handoff is:

1. A recovery checkpoint snapshots visibility, file/buffer coverage and pending
   segment replay boundaries, then atomically publishes its pointer.
2. The installer verifies each installed row in durable REGULAR storage,
   MainIndex, supported business indexes and checkpoint coverage.
3. Full batch plans are atomically replaced by compact transaction checkpoints
   containing table identity, commit timestamp, row-id ranges and covered files.
4. The participant durably writes CHECKPOINTED and evicts the transaction's
   cached payload. Background reclamation groups retired transactions and copies
   live records into a new generation once retired payload bytes reach live
   payload bytes. Its pointer is durable before an old WAL is deleted. A restart
   between the fence and reclamation preserves both the fence and live inputs.
5. The coordinator retires PUBLISHED/ABORTED decisions only after every owner
   acknowledges checkpoint/discard.

Late requests remain fenced by retained terminal records. Expired records are
compacted to a durable retired transaction-ID high-water mark, so removed IDs
cannot be recreated. Retention must be at least the transaction lease and
should cover the maximum expected client delay.

MainIndex durability remains the existing SQLite database; it is not copied
into the recovery-checkpoint body. Transaction checkpoints verify that database
before private WAL/plan removal. Storage-GC relocation retains a separate WAL
until the replacement file and recovery checkpoint assume responsibility.

## Rewriting Storage GC

Storage GC continues to use StorageGarbageCollector, existing visibility and
indexes. Rewrites preserve surviving row IDs while relocating MainIndex
positions. Business-index membership therefore remains exact, including
duplicate members in non-unique indexes and keyless tables.

The Storage-GC WAL records old locations before relocation. Recovery rolls back
an unpublished swap or completes a committed swap. Publication is serialized
with process-local read pins; a pinned ReadView retains its physical coverage.
Retired files are not deleted while a read pin or incomplete GC WAL task depends
on them. Exact old-file MainIndex entries are removed only during retirement.

## Verification

Pixels requires JDK 8; Trino 466 requires JDK 23. Build native code from the
matching Pixels source. Never preload jemalloc into Trino/helper processes.

### Normal production service classes

With JDK 8 and pinned etcd 3.5:

~~~sh
PIXELS_HOME=/path/to/matching/runtime \
ETCD_BIN=/path/to/etcd \
bash tools/verify-ingest-daemon.sh
~~~

This starts isolated real etcd and production TransServer, RetinaServer and
ServerContainer in two independent backend JVMs over one locked state volume.
The first process commits 64 rows, commits one still-buffered row, and commits
two FILE transactions into one shared output. It waits for a
recovery checkpoint, verifies that the first transaction's full plan and WAL
payload were physically replaced by compact checkpoint/fence state, checks the
legacy write fence, and shuts down cleanly. Before starting services, the second
process opens the same catalog, plan and WAL state and validates the handoff; it
then recovers the PUBLISHED decisions, reaches READY, shuts down, and reads the
Pixels files directly to prove an exact 73-row multiset without replay duplicates.
It finally corrupts a copied committed-decision state and removes the published
checkpoint body in turn; separate daemon attempts must fail before READY with an
actionable checksum or missing-body error. An offline cutover phase first proves
that a transaction allocator below the configured baseline is rejected. It then
advances the isolated etcd ID domain while services are stopped, preserves all
73 old physical rows, commits one new row above baseline 1000000000, and rechecks
the legacy mutation fence. Catalog persistence and topology discovery remain
fixtures.

~~~text
PIXELS_NORMAL_INGEST_DAEMON_PHASE1_PASS rows=73 sharedFileTransactions=2 checkpointedTransaction=5
PIXELS_NORMAL_INGEST_DAEMON_PASS rows=73 pixelsFiles=3 services=TransServer,RetinaServer checkpointRestart=2 sharedFileTransactions=2
PIXELS_NORMAL_INGEST_CUTOVER_PASS oldRows=73 totalRows=74 baseline=1000000000 commitTimestamp=1000000002 legacyFence=1
PIXELS_NORMAL_INGEST_FAIL_CLOSED_PASS corruptDecision=1 missingCheckpoint=1 allocatorFloor=1
~~~

### Full SQL end-to-end

With JDK 23:

~~~sh
PIXELS_HOME=/path/to/matching/runtime \
bash tools/verify-sql-insert.sh /path/to/matching/pixels
~~~

Use SQL_E2E_SKIP_BUILD=1 only after both projects and native runtime were rebuilt
from exact checked-out sources. SQL_E2E_WORK_DIR selects the evidence directory.
`SQL_E2E_ENGINE_HEAP` (default `3g`) and `SQL_E2E_BACKEND_HEAP` (default `1g`)
size the independent Trino and Retina JVMs. Large-file benchmarks must budget
for concurrent file readers as well as ingestion; the SF10 profile uses `8g`
for the engine and `6g` for Retina.

The harness uses real Trino 466 coordinator/two-worker servers, actual plugin
classloader and connector, TCP/gRPC, decisions/journals, Retina MemTables,
native visibility, SQLite MainIndex and Pixels readers/writers. Catalog, node
directory and external identity allocation are fixtures. It is one-host,
one-Retina-owner integration, not HA certification.

Assertions cover VALUES, duplicate preservation, NULLs, reordered/omitted
columns, zero-row and 500-row INSERT SELECT, same-table INSERT SELECT,
concurrent commits, buffer/file handoff and a 480-row private staging failure
followed by durable Abort. JDBC tests keep one connection across START
TRANSACTION, verify multi-statement/multi-table private reads and atomic
publication, exercise rollback and rejected transaction modes, and run once
with VISIBLE acknowledgement and once with DURABLE plus an explicit visibility
barrier.

The September 11, 2026 baseline
[run 34571751533](https://github.com/AntiO2/pixels-trino/actions/runs/34571751533)
used Pixels 1d536d150e6ef47f7c3312514ede3f21023a42d8 and Pixels-Trino
91297500943dc5fd34c3bcee8cc7ec043bc6b1aa:

~~~text
FULL_SQL_INSERT_E2E_PASS checks=20 rows=1008 trinoWorkers=2 appendRPCs=53 bufferReadRPCs=11 fileVisibilityRPCs=3 pixelsFiles=1 privateAbortedRows=480
~~~

### Focused lifecycle/GC tests

From Pixels:

~~~sh
bash tools/verify-ingest-contract.sh
bash tools/verify-ingest-gc.sh
~~~

TestPixelsIngestStorage uses real Pixels files, native visibility and SQLite to
verify read-pin rollback, stable row relocation, physical retirement,
checkpointed GC-WAL deletion and restart of journal/installer/index objects.
Coordinator tests cover terminal-fence compaction/high-water rejection; journal
tests inject crashes around WAL generation publication. TestRecoveryCheckpoint
covers 29 body-codec, pointer-publication, replacement and corrupt/missing-state
cases; it is part of the Full SQL CI lifecycle regression set.

### TPC-H and TPC-DS INSERT coverage

#### Full-column SF10 benchmark

Build Pixels with JDK 8 and the connector with JDK 23 as described above, then
run with JDK 23 and the matching native runtime:

~~~sh
PIXELS_HOME=/path/to/pixels-home ETCD_BIN=/path/to/etcd SQL_E2E_SKIP_BUILD=1 \
  bash tools/benchmark-all-tpc-tables.sh /path/to/pixels
~~~

This entry point defaults to `tpch.sf10` and `tpcds.sf10`, all 32 tables and
all columns, two Trino workers, four vnodes, and FILE + DURABLE. It creates a
fresh isolated environment and never resumes a previous import. Each table
uses numeric-key ranges targeting 10 million rows per INSERT, configurable
with `ALL_TPC_TRANSACTION_ROWS`. Each INSERT is an independent transaction;
a visibility barrier runs once after all INSERTs for that table. Accepted time
is the sum of INSERT execution times through their durable commit acknowledgements;
commit-to-visible latency measures the barrier wait after the final INSERT.
Source generation during INSERT is
included, while separate source scans and result verification are excluded.
Every table is checked using row count and an order-independent full-column
checksum.

The defaults reserve 6 GiB heap each for engine and backend, allow up to
20 million rows per transaction, reserve up to 1 GiB of installation-plan
capacity, bound WAL payload to
64 GiB, and allow four
hours for the run. Batch payload is capped at 1 MiB to leave room for the RPC
envelope. The script exposes these settings as environment overrides. The row
target is approximate because equal-width key ranges need not have equal row
counts; the log records the actual transaction count and largest transaction.
The prepared-row limit includes headroom over the target.

The existing benchmark lifecycle starts its own etcd process and enables
recovery checkpoints and WAL reclamation. It does not use a shared etcd instance.

The JDBC client, Trino servers, connector, TCP/gRPC, WAL, installation, indexes,
visibility and Pixels file I/O are real. Metadata, node discovery and external
ID allocation use the SQL fixture. Its dynamic benchmark table definitions
are process-scoped; use the normal-server entry point for restart verification.
CI explicitly selects `TPCH_SCHEMA=tiny TPCDS_SCHEMA=tiny` for all-column
correctness coverage; those results are not SF10 performance measurements.

Run the representative two-worker latency benchmark in an isolated environment:

~~~sh
PIXELS_HOME=/path/to/matching/runtime \
bash tools/benchmark-tpch-tpcds-insert.sh /path/to/matching/pixels
~~~

Against an already running normal Trino and Retina deployment, copy and verify
all eight TPC-H and all 24 TPC-DS tiny tables:

~~~sh
JAVA_HOME=/path/to/jdk-23 \
bash tools/verify-all-tpc-tables.sh \
  jdbc:trino://127.0.0.1:18081 \
  /persistent/pixels/tables \
  tpc_insert_all

JAVA_HOME=/path/to/jdk-23 ALL_TPC_VERIFY_ONLY=true \
bash tools/verify-all-tpc-tables.sh \
  jdbc:trino://127.0.0.1:18081 \
  /persistent/pixels/tables \
  tpc_insert_all
~~~

For the complete SF100 `FILE + DURABLE` measurement, configure the Retina
deployment with `retina.ingest.write.representation=FILE` and
`retina.ingest.commit.ack=DURABLE`, then run:

~~~sh
JAVA_HOME=/path/to/jdk-23 \
TPCH_SCHEMA=sf100 TPCDS_SCHEMA=sf100 \
ALL_TPC_TRANSACTION_ROWS=20000000 \
ALL_TPC_BARRIER_EACH_TRANSACTION=false \
ALL_TPC_VISIBILITY_TIMEOUT_SECONDS=3600 \
bash tools/verify-all-tpc-tables.sh \
  jdbc:trino://127.0.0.1:18081 \
  /persistent/pixels/tables \
  tpc_sf100 | tee tpc-sf100-file-durable.log
~~~

The transaction-row target divides large tables into deterministic numeric-key
ranges. Each range is a separate DURABLE transaction. The SF100 performance
configuration accepts all ranges asynchronously and uses one fixed-boundary
visibility barrier per table; the default correctness configuration retains a
barrier after every transaction. Each table prints its source scan time,
DURABLE accepted time and throughput, visibility time, transaction count and
largest observed transaction. Set `ALL_TPC_RESUME=true` to verify completed
target tables and continue from the first missing deterministic transaction
range in a partially imported table after an interruption. A non-empty partial
range fails closed because each range is one atomic transaction. The prepared-row
and WAL limits must be sized for the largest
observed transaction before starting the run.

### SF10 ingestion and native writer comparison

The representative benchmark projects `lineitem` to `(orderkey, comment)`
and `store_sales` to `(ss_ticket_number, ss_item_sk-as-text)`, with a dataset
prefix on the text column. These results measure those two-column projections.
`INSERT_BENCHMARK_STAGE_SOURCE=true` materializes each projection in Trino's
memory catalog before the ingestion timer, avoiding repeated data generation
for concurrent key ranges. Staging and final checksum verification are timed
separately. The default `false` reads the generators directly.

Use fresh work directories for every measurement. With matching artifacts
already built under JDK 8 (Pixels) and JDK 23 (Trino), run under JDK 23:

~~~sh
export PIXELS_HOME=/path/to/matching/runtime
BENCHMARK_ROOT=$(mktemp -d)
SQL_E2E_SKIP_BUILD=1 SQL_E2E_WORK_DIR="$BENCHMARK_ROOT/sql" \
SQL_E2E_TIMEOUT_SECONDS=14400 \
SQL_E2E_ENGINE_HEAP=8g SQL_E2E_BACKEND_HEAP=6g \
SQL_E2E_BACKEND_STOP_TIMEOUT_SECONDS=180 \
TPCH_SCHEMA=sf10 TPCDS_SCHEMA=sf10 \
INSERT_BENCHMARK_STAGE_SOURCE=true \
INSERT_BENCHMARK_CONCURRENCY=8 INSERT_BENCHMARK_TRANSACTION_ROWS=5000000 \
INSERT_BENCHMARK_VISIBILITY_TIMEOUT_SECONDS=7200 \
PIXELS_SQL_FIXTURE_REPRESENTATION=FILE PIXELS_SQL_FIXTURE_COMMIT_ACK=DURABLE \
PIXELS_SQL_FIXTURE_ROUTE_COUNT=4 PIXELS_SQL_FIXTURE_MAX_BATCH_ROWS=65536 \
PIXELS_SQL_FIXTURE_MAX_BATCH_BYTES=16777216 \
PIXELS_SQL_FIXTURE_MAX_PREPARED_ROWS=10000000 \
PIXELS_SQL_FIXTURE_MAX_STATE_BYTES=1073741824 \
PIXELS_SQL_FIXTURE_WAL_MAX_BYTES=68719476736 \
PIXELS_SQL_FIXTURE_WAL_MAX_RECORDS=100000000 \
PIXELS_SQL_FIXTURE_WAL_READ_CACHE_MAX_BYTES=268435456 \
PIXELS_SQL_FIXTURE_FILE_TARGET_ROWS=4000000 \
PIXELS_SQL_FIXTURE_FILE_MAX_BYTES=134217728 \
PIXELS_SQL_FIXTURE_FILE_MAX_DELAY_MS=300000 \
bash tools/benchmark-tpch-tpcds-insert.sh /path/to/pixels

# Build pixels-cli with JDK 8 first; execute this harness with JDK 23.
bash tools/benchmark-native-load.sh /path/to/pixels \
  "$BENCHMARK_ROOT/sql" tpch "$BENCHMARK_ROOT/native-tpch"
bash tools/benchmark-native-load.sh /path/to/pixels \
  "$BENCHMARK_ROOT/sql" tpcds "$BENCHMARK_ROOT/native-tpcds"
~~~

The native comparison exports the verified projection before its timer, then
runs the existing CLI `SimplePixelsConsumer` with four threads and real
PixelsWriter output. Metadata uses the catalog fixture. CSV preparation is
excluded, input is warm, and output footer row counts are checked. This baseline
does not perform transactional WAL or MainIndex installation.

Accepted time ends after all INSERTs return with durable COMMIT decisions.
Commit-to-visible starts there and ends when the fixed-boundary barrier returns.
It is a latency, not a throughput. Compare native-writer and transactional
results only when both use the same projection and input preparation.

Keyless PageSinks combine successive Trino Pages up to the configured batch
row or byte limit, account for retained rows, and apply backpressure while
sending a full batch. Finish sends the remaining rows before sealing streams.
Indexed routing retains its per-Page batching path.

The optimized path uses a bounded private batch cache after durable sealing,
batch MainIndex installation for fresh allocations, cached batch type parsing,
and grouped WAL reclamation. SQLite MainIndex retains continuous bulk mappings
as `RowIdRange` objects in its existing write buffer; overlapping batches retain
per-entry duplicate results, and buffered mappings are discarded only after a
successful durable flush. Fresh FILE spans pass continuous mappings directly
through LocalIndexService to MainIndex without constructing per-row messages.
Prepare validates each input batch in one pass; publication polling does not
reread installed payloads, and checkpoint retains coverage descriptors rather
than the transaction's full payload. Immutable payload bytes are shared with RPC and
WAL encoding rather than converted through intermediate mutable arrays.
For FILE tables without business indexes, validated columnar payloads feed
bounded PixelsWriter vector windows directly; variable-width vectors reference
payload slices until the writer consumes the window. This path does not use
the public MemTable. Recovery retains exact-location verification; cache misses
and restarts read the WAL. Accepted throughput is not completed materialization.

### Small transaction benchmark

`SmallTransactionsInsert` drives independent INSERTs through Trino JDBC and
reports the accepted boundary separately from the final visibility barrier:

~~~sh
JAVA_HOME=/path/to/jdk-23 \
SMALL_INSERT_TRANSACTIONS=1000 \
SMALL_INSERT_CONCURRENCY=16 \
SMALL_INSERT_TRANSACTION_MODE=AUTOCOMMIT \
bash tools/benchmark-small-inserts.sh \
  jdbc:trino://127.0.0.1:18081 benchmark_schema events pixels
~~~

Set `SMALL_INSERT_TRANSACTION_MODE=EXPLICIT` to execute each INSERT followed by
JDBC `commit()` on the same connection. Configure the target deployment for
FILE representation and DURABLE acknowledgement when measuring asynchronous
ingestion. Use a fresh target table for each run.

To run against the isolated SQL fixture, use the matching JDK 23 runtime and
prebuilt Pixels/Trino artifacts:

~~~sh
SQL_E2E_SKIP_BUILD=1 SQL_E2E_WORK_DIR="$(mktemp -d)" \
SQL_E2E_MAIN_CLASS=io.pixelsdb.pixels.trino.testing.SmallTransactionsInsert \
SQL_E2E_PASS_PATTERN=SMALL_TRANSACTION_INSERT_PASS \
SQL_E2E_TIMEOUT_SECONDS=1800 SQL_E2E_BACKEND_HEAP=2g \
SMALL_INSERT_CREATE_TABLE=false SMALL_INSERT_TRANSACTIONS=1000 \
SMALL_INSERT_CONCURRENCY=16 SMALL_INSERT_TRANSACTION_MODE=EXPLICIT \
PIXELS_SQL_FIXTURE_REPRESENTATION=FILE PIXELS_SQL_FIXTURE_COMMIT_ACK=DURABLE \
PIXELS_SQL_FIXTURE_ROUTE_COUNT=4 PIXELS_SQL_FIXTURE_FILE_MAX_DELAY_MS=300000 \
bash tools/verify-sql-insert.sh /path/to/pixels
~~~

The harness validates final row count and ID sum. It reports accepted
throughput, client latency percentiles, INSERT query elapsed time, explicit
COMMIT latency and commit-to-visible latency separately. Percentiles are not
additive; query CPU time does not include coordinator planning time.

Optional diagnostics:

- `SMALL_INSERT_QUERY_DIAGNOSTICS=true` captures engine query phases in the
  embedded JDBC harness. Trino 466 measures execution from planning start
  through query end, so phase timings overlap.
- `PIXELS_SQL_FIXTURE_RPC_TIMING=true` records per-method call counts, total
  nanoseconds and maximum nanoseconds in `status.properties`. These are
  server-handler timings: network transport and pre-dispatch queueing are
  excluded, while warmups and background RPCs are included.

Both diagnostics are disabled by default. Keep their settings consistent
between comparison runs. Preserve the source revisions, configuration and
result logs with benchmark reports; generated datasets can be removed after
verification and any native-writer comparisons are complete.

### All-table restart verification

For the all-table verification commands above, run the verify-only command after
the configured buffer flush interval and again after stopping and restarting
the normal Coordinator and Retina JVMs. It
compares row counts and an order-independent checksum of every column. Before
restart, record the recovery-checkpoint pointer and verify that WAL and
installer state have actually shrunk; a READY message alone is insufficient.

`retina.ingest.max.prepared.rows` limits one prepared transaction, not a table
or MemTable. Size it above the largest supported single INSERT while retaining
a bounded failure domain. TPC-DS tiny `customer_demographics` contains
1,920,800 rows, so the default 1,000,000-row limit deliberately rejects that
one-statement load; the all-table verification uses 3,000,000.

Recovery preserves the recorded file identity when a plan crosses a file
boundary. Local object reads copy reader-owned direct/mapped memory before
closing the reader; callers must not retain buffers owned by a closed reader.

### Local Trino smoke test

For a deployment whose Trino HTTP port is 18081:

~~~sh
/path/to/trino --server http://127.0.0.1:18081 \
  --catalog pixels --schema demo
~~~

Then create a dedicated path and table and exercise immediate visibility:

~~~sql
CREATE SCHEMA IF NOT EXISTS pixels.demo;
CREATE TABLE pixels.demo.insert_smoke (
  id bigint,
  label varchar,
  amount decimal(12, 2),
  created date
) WITH (
  storage = 'file',
  paths = 'file:///persistent/pixels/tables/demo/insert_smoke'
);
INSERT INTO pixels.demo.insert_smoke VALUES
  (1, 'alpha', DECIMAL '12.30', DATE '2026-09-13'),
  (2, NULL, NULL, NULL);
SELECT * FROM pixels.demo.insert_smoke ORDER BY id;
~~~

Create a new empty directory for a new table. Do not point `paths` at another
table or change the offline-cutover baseline while services are live.

## Remaining limitations

- No replicated coordinator/participant log or automatic HA owner failover.
- Fixed topology, a persistent catalog fixture and process-local read leases;
  multi-owner GC is not certified.
- No Trino task-attempt retry protocol.
- UPDATE/DELETE MergeSink is not implemented.
- Unique secondary constraints are rejected.
- Full SQL still uses catalog, topology and external-ID fixtures; production
  metadata/container deployment needs an environment-specific test.
