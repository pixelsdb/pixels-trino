package io.pixelsdb.pixels.trino.testing;

import static io.trino.testing.TestingSession.testSessionBuilder;

import io.trino.metadata.HandleResolver;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** End-to-end generated TPC data INSERT benchmark with concurrent visibility checks. */
public final class TpchTpcdsInsertBenchmark {
    private static final long DEFAULT_VISIBILITY_TIMEOUT_SECONDS =
            TimeUnit.HOURS.toSeconds(2L);
    private static final String STAGED_SOURCE_TABLE = "memory.default.insert_source";

    private record SourceResult(long rows, String checksum, long elapsedMillis) {}

    private record Dataset(
            String name,
            String source,
            String projection,
            String chunkColumn,
            String targetPredicate) {}

    private record Bounds(long minimum, long maximum) {}

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int positiveInteger(String name, int defaultValue) {
        int value = Integer.parseInt(environment(name, Integer.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long positiveLong(String name, long defaultValue) {
        long value = Long.parseLong(environment(name, Long.toString(defaultValue)));
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonNegativeLong(String name, long defaultValue) {
        long value = Long.parseLong(environment(name, Long.toString(defaultValue)));
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static String identifier(String name, String defaultValue) {
        String value = environment(name, defaultValue);
        if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(name + " is not a SQL identifier: " + value);
        }
        return value;
    }

    private static boolean environmentBoolean(String name, boolean defaultValue) {
        return Boolean.parseBoolean(environment(name, Boolean.toString(defaultValue)));
    }

    private static long scalar(DistributedQueryRunner runner, String sql) {
        return ((Number) runner.execute(sql).getOnlyValue()).longValue();
    }

    private static SourceResult sourceResult(DistributedQueryRunner runner, Dataset dataset) {
        long start = System.nanoTime();
        MaterializedRow row =
                runner.execute(
                                "SELECT count(*), to_hex(checksum(ROW(" + dataset.projection()
                                        + "))) FROM " + dataset.source())
                        .getMaterializedRows()
                        .get(0);
        return new SourceResult(
                ((Number) row.getField(0)).longValue(),
                String.valueOf(row.getField(1)),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    private static String targetChecksum(DistributedQueryRunner runner, Dataset dataset) {
        return String.valueOf(
                runner.execute(
                                "SELECT to_hex(checksum(ROW(id, label))) FROM pixels.s.t WHERE "
                                        + dataset.targetPredicate())
                        .getOnlyValue());
    }

    private static long targetCount(DistributedQueryRunner runner, Dataset dataset) {
        return scalar(
                runner,
                "SELECT count(*) FROM pixels.s.t WHERE " + dataset.targetPredicate());
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static Bounds bounds(DistributedQueryRunner runner, Dataset dataset) {
        MaterializedRow row = runner.execute(
                        "SELECT min(" + dataset.chunkColumn() + "), max("
                                + dataset.chunkColumn() + ") FROM " + dataset.source())
                .getMaterializedRows()
                .get(0);
        return new Bounds(
                ((Number) row.getField(0)).longValue(),
                ((Number) row.getField(1)).longValue());
    }

    private static long metric(Path control, String name) throws Exception {
        Properties properties = new Properties();
        try (java.io.InputStream input =
                Files.newInputStream(control.resolve("status.properties"))) {
            properties.load(input);
        }
        return Long.parseLong(properties.getProperty(name));
    }

    private static long runDataset(
            DistributedQueryRunner runner, Dataset dataset, int concurrency,
            long transactionRows, long visibilityTimeoutSeconds, boolean stageSource)
            throws Exception {
        SourceResult source = sourceResult(runner, dataset);
        if (source.rows() <= 0) {
            throw new AssertionError(dataset.name() + " source is empty");
        }
        if (targetCount(runner, dataset) != 0) {
            throw new AssertionError(dataset.name() + " target tag is not empty");
        }

        Dataset inputDataset = dataset;
        if (stageSource) {
            long stagingStart = System.nanoTime();
            long stagedRows = runner.execute("CREATE TABLE " + STAGED_SOURCE_TABLE
                            + " AS SELECT id, label FROM (SELECT " + dataset.projection()
                            + " FROM " + dataset.source() + ") input(id, label)")
                    .getUpdateCount().orElseThrow();
            if (stagedRows != source.rows()) {
                throw new AssertionError("Staged source row count differs");
            }
            System.out.println(String.format(java.util.Locale.ROOT,
                    "TPC_SOURCE_STAGED dataset=%s rows=%d stagingMs=%d",
                    dataset.name(), stagedRows, millis(System.nanoTime() - stagingStart)));
            inputDataset = new Dataset(dataset.name(), STAGED_SOURCE_TABLE,
                    "id, label", "id", dataset.targetPredicate());
        }
        final Dataset input = inputDataset;
        Bounds bounds = bounds(runner, input);
        long keyCount = Math.addExact(Math.subtractExact(bounds.maximum(), bounds.minimum()), 1L);
        long desiredTransactions = transactionRows == 0L
                ? concurrency
                : Math.max(1L, (source.rows() + transactionRows - 1L) / transactionRows);
        long rangeWidth = Math.max(
                1L, (keyCount + desiredTransactions - 1L) / desiredTransactions);
        ExecutorService inserts = Executors.newFixedThreadPool(concurrency);
        List<Future<Long>> tasks = new ArrayList<>();
        long insertStart = System.nanoTime();
        for (long lower = bounds.minimum(); lower <= bounds.maximum();
                lower = Math.addExact(lower, rangeWidth)) {
            long rangeStart = lower;
            long rangeEnd = Math.min(
                    bounds.maximum(), Math.addExact(rangeStart, rangeWidth - 1L));
            tasks.add(inserts.submit(() -> runner.execute(
                            "INSERT INTO pixels.s.t SELECT " + input.projection()
                                    + " FROM " + input.source() + " WHERE "
                                    + input.chunkColumn() + " BETWEEN " + rangeStart
                                    + " AND " + rangeEnd)
                    .getUpdateCount()
                    .orElseThrow()));
            if (rangeEnd == bounds.maximum()) {
                break;
            }
        }
        long affected = 0;
        try {
            for (Future<Long> task : tasks) {
                affected = Math.addExact(affected, task.get());
            }
        } finally {
            inserts.shutdownNow();
        }
        long insertReturned = System.nanoTime();
        if (affected != source.rows()) {
            throw new AssertionError(
                    dataset.name() + " affected=" + affected + ", source=" + source.rows());
        }

        long barrierStart = System.nanoTime();
        runner.execute("CALL pixels.system.flush_visible_barrier(timeout_seconds => "
                + visibilityTimeoutSeconds + ")");
        long visibleAt = System.nanoTime();
        long visibleCount = targetCount(runner, dataset);
        String visibleChecksum = targetChecksum(runner, dataset);
        long verificationEnd = System.nanoTime();
        if (visibleCount != source.rows() || !source.checksum().equals(visibleChecksum)) {
            throw new AssertionError(
                    dataset.name() + " visible result mismatch: rows=" + visibleCount
                            + ", checksum=" + visibleChecksum + ", expected=" + source.checksum());
        }

        long insertNanos = insertReturned - insertStart;
        double rowsPerSecond = source.rows() * 1_000_000_000.0 / insertNanos;
        String result = String.format(
                java.util.Locale.ROOT,
                "TPC_INSERT_RESULT dataset=%s rows=%d sourceScanMs=%d insertMs=%d rowsPerSecond=%.2f "
                        + "transactions=%d commitToVisibleMs=%d verificationMs=%d checksum=%s sourceMode=%s%n",
                dataset.name(),
                source.rows(),
                source.elapsedMillis(),
                millis(insertNanos),
                rowsPerSecond,
                tasks.size(),
                millis(visibleAt - insertReturned),
                millis(verificationEnd - visibleAt),
                source.checksum(), stageSource ? "STAGED" : "GENERATED");
        System.out.println(result);
        if (stageSource) {
            runner.execute("DROP TABLE " + STAGED_SOURCE_TABLE);
        }
        return source.rows();
    }

    public static void main(String[] args) throws Exception {
        Path control = Paths.get(args[0]);
        int workers = positiveInteger("INSERT_BENCHMARK_WORKERS", 2);
        int concurrency = positiveInteger("INSERT_BENCHMARK_CONCURRENCY", 1);
        long transactionRows = nonNegativeLong(
                "INSERT_BENCHMARK_TRANSACTION_ROWS", 0L);
        long visibilityTimeoutSeconds = positiveLong(
                "INSERT_BENCHMARK_VISIBILITY_TIMEOUT_SECONDS",
                DEFAULT_VISIBILITY_TIMEOUT_SECONDS);
        String tpchSchema = identifier("TPCH_SCHEMA", "tiny");
        String tpcdsSchema = identifier("TPCDS_SCHEMA", "tiny");
        boolean includeTpcds = environmentBoolean("INSERT_BENCHMARK_INCLUDE_TPCDS", true);
        boolean stageSource = environmentBoolean("INSERT_BENCHMARK_STAGE_SOURCE", false);

        List<URL> pluginUrls = new ArrayList<>();
        for (String path :
                Files.readString(Paths.get(args[1])).trim().split(java.io.File.pathSeparator)) {
            pluginUrls.add(Paths.get(path).toUri().toURL());
        }
        try (PluginClassLoader pluginLoader =
                        PluginManager.createClassLoader("pixels-tpc-insert", pluginUrls);
                DistributedQueryRunner runner =
                        DistributedQueryRunner.builder(
                                        testSessionBuilder()
                                                .setCatalog("pixels")
                                                .setSchema("s")
                                                .build())
                                .setWorkerCount(workers)
                                .build()) {
            for (io.trino.server.testing.TestingTrinoServer server : runner.getServers()) {
                server.getInstance(com.google.inject.Key.get(HandleResolver.class))
                        .registerClassLoader(pluginLoader);
            }
            Plugin pixels =
                    (Plugin)
                            pluginLoader
                                    .loadClass("io.pixelsdb.pixels.trino.PixelsPlugin")
                                    .getConstructor()
                                    .newInstance();
            try (io.trino.spi.classloader.ThreadContextClassLoader ignored =
                    new io.trino.spi.classloader.ThreadContextClassLoader(pluginLoader)) {
                runner.installPlugin(pixels);
            }
            runner.createCatalog(
                    "pixels",
                    "pixels",
                    Map.of("cloud.function.switch", "off", "insert.enabled", "true"));
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch", Map.of("tpch.splits-per-node", "4"));
            runner.installPlugin(new TpcdsPlugin());
            runner.createCatalog("tpcds", "tpcds");
            if (stageSource) {
                runner.installPlugin(new MemoryPlugin());
                runner.createCatalog("memory", "memory", Map.of(
                        "memory.max-data-per-node",
                        environment("INSERT_BENCHMARK_STAGED_SOURCE_MAX_DATA", "3GB")));
            }

            Dataset tpch =
                    new Dataset(
                            "tpch." + tpchSchema + ".lineitem",
                            "tpch." + tpchSchema + ".lineitem",
                            "orderkey, CAST('tpch:' || comment AS varchar)",
                            "orderkey",
                            "label LIKE 'tpch:%'");
            Dataset tpcds =
                    new Dataset(
                            "tpcds." + tpcdsSchema + ".store_sales",
                            "tpcds." + tpcdsSchema + ".store_sales",
                            "ss_ticket_number, CAST('tpcds:' || COALESCE(CAST(ss_item_sk AS varchar), '<null>') AS varchar)",
                            "ss_ticket_number",
                            "label LIKE 'tpcds:%'");

            long tpchRows = runDataset(
                    runner, tpch, concurrency, transactionRows, visibilityTimeoutSeconds, stageSource);
            long tpcdsRows = includeTpcds
                    ? runDataset(
                            runner, tpcds, concurrency, transactionRows,
                            visibilityTimeoutSeconds, stageSource)
                    : 0L;
            long totalRows = tpchRows + tpcdsRows;
            if (scalar(runner, "SELECT count(*) FROM pixels.s.t") != totalRows) {
                throw new AssertionError("combined target row count mismatch");
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while ((metric(control, "pixelsFiles") == 0
                            || metric(control, "activeTransactions") != 0)
                    && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            if (metric(control, "pixelsFiles") == 0) {
                throw new AssertionError("benchmark did not materialize a Pixels file");
            }
            if (metric(control, "activeTransactions") != 0) {
                throw new AssertionError("benchmark transactions were not checkpointed and retired");
            }
            if (targetCount(runner, tpch) != tpchRows
                    || (includeTpcds && targetCount(runner, tpcds) != tpcdsRows)) {
                throw new AssertionError("file/buffer handoff changed the target multiset");
            }
            System.out.println(
                    "TPC_INSERT_BENCHMARK_PASS tpchRows=" + tpchRows
                            + " tpcdsRows=" + tpcdsRows
                            + " totalRows=" + totalRows
                            + " workers=" + workers);
        }
    }
}
