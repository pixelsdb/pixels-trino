package io.pixelsdb.pixels.trino.testing;

import static io.trino.testing.TestingSession.testSessionBuilder;

import io.trino.metadata.HandleResolver;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;

import java.net.URL;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

/** Runs actual SQL against the real connector, RPC participant, and Pixels storage. */
public final class FullSqlInsert {
    private static final long EXPLICIT_A_ROWS = 4L;
    private static final long EXPLICIT_B_ROWS = 2L;
    private static final long EXPLICIT_TOTAL_ROWS = EXPLICIT_A_ROWS + EXPLICIT_B_ROWS;
    private static final long EXPLICIT_ROLLBACK_STAGED_ROWS = 1L;
    private static final long EXPLICIT_FAILED_PREFIX_ROWS = 1L;
    private static final long FAILED_STATEMENT_STAGED_ROWS = 480L;
    private static final long EXPLICIT_FAILED_TRANSACTION_STAGED_ROWS =
            EXPLICIT_FAILED_PREFIX_ROWS + FAILED_STATEMENT_STAGED_ROWS;
    private static final long AUTOCOMMIT_TOTAL_ROWS = 1_008L;
    private static final long SUCCESSFUL_ACCEPTED_ROWS =
            EXPLICIT_TOTAL_ROWS + AUTOCOMMIT_TOTAL_ROWS;
    private static final long STATUS_POLL_INTERVAL_MILLIS = 50L;
    private static final long SHORT_METRIC_DEADLINE_SECONDS = 5L;
    private static final long FILE_MATERIALIZATION_DEADLINE_SECONDS = 20L;
    private static final long FILE_COUNT_STABILITY_MILLIS = 500L;
    private static final long DEFAULT_VISIBILITY_BARRIER_TIMEOUT_SECONDS = 30L;
    private static final long VISIBILITY_BARRIER_TIMEOUT_SECONDS =
            Long.getLong(
                    "pixels.sql.visibility-barrier-timeout-seconds",
                    DEFAULT_VISIBILITY_BARRIER_TIMEOUT_SECONDS);
    private static final boolean RUN_FAILURE_SCENARIOS =
            Boolean.parseBoolean(System.getProperty("pixels.sql.run-failure-scenarios", "true"));
    private static final long FILE_AGGREGATION_ID_BASE = 300_000L;

    private static final java.util.concurrent.atomic.AtomicInteger checks =
            new java.util.concurrent.atomic.AtomicInteger();

    private static void equal(long actual, long expected, String context) {
        if (actual != expected) {
            throw new AssertionError(context + ": expected=" + expected + ", actual=" + actual);
        }
        checks.incrementAndGet();
    }

    private static long scalar(DistributedQueryRunner runner, String sql) {
        return ((Number) runner.execute(sql).getOnlyValue()).longValue();
    }

    private static void insert(DistributedQueryRunner runner, String sql, long rows) {
        MaterializedResult result = runner.execute(sql);
        equal(result.getUpdateCount().orElseThrow(), rows, sql);
        flushVisible(runner);
        System.out.println("SQL_PASS affected=" + rows + " sql=" + sql);
    }

    private static void flushVisible(DistributedQueryRunner runner) {
        runner.execute("CALL pixels.system.flush_visible_barrier(timeout_seconds => "
                + VISIBILITY_BARRIER_TIMEOUT_SECONDS + ")");
    }

    private static long scalar(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new AssertionError("Query returned no rows: " + sql);
            }
            long value = result.getLong(1);
            if (result.next()) {
                throw new AssertionError("Scalar query returned multiple rows: " + sql);
            }
            return value;
        }
    }

    private static void update(Connection connection, String sql, long expectedRows)
            throws Exception {
        try (Statement statement = connection.createStatement()) {
            equal(statement.executeUpdate(sql), expectedRows, sql);
        }
    }

    private static void execute(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static int optionalPositiveEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            return 0;
        }
        int parsed = Integer.parseInt(value.trim());
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive when set");
        }
        return parsed;
    }

    private static long verifyFileAggregation(DistributedQueryRunner runner, Path control)
            throws Exception {
        int transactions = optionalPositiveEnvironment(
                "PIXELS_SQL_FILE_AGGREGATION_TRANSACTIONS");
        if (transactions == 0) {
            return 0;
        }
        int rowsPerTransaction = optionalPositiveEnvironment(
                "PIXELS_SQL_FILE_AGGREGATION_ROWS_PER_TRANSACTION");
        if (rowsPerTransaction == 0) {
            throw new IllegalArgumentException(
                    "PIXELS_SQL_FILE_AGGREGATION_ROWS_PER_TRANSACTION is required");
        }
        long rows = Math.multiplyExact((long) transactions, rowsPerTransaction);
        flushVisible(runner);
        long filesBefore = awaitStableMetric(control, "primaryTablePixelsFiles");
        long acceptedStart = System.nanoTime();
        for (int transaction = 0; transaction < transactions; transaction++) {
            long base = Math.addExact(FILE_AGGREGATION_ID_BASE,
                    Math.multiplyExact((long) transaction, rowsPerTransaction));
            MaterializedResult result = runner.execute(
                    "INSERT INTO t SELECT " + base + "+n, 'file-aggregate'"
                            + " FROM UNNEST(sequence(1," + rowsPerTransaction + ")) AS u(n)");
            equal(result.getUpdateCount().orElseThrow(), rowsPerTransaction,
                    "FILE aggregation transaction " + transaction);
        }
        long acceptedEnd = System.nanoTime();
        flushVisible(runner);
        long visibleEnd = System.nanoTime();
        equal(scalar(runner, "SELECT count(*) FROM t WHERE label='file-aggregate'"),
                rows, "cross-transaction FILE aggregation rows");
        long filesAfter = awaitStableMetric(control, "primaryTablePixelsFiles");
        equal(filesAfter - filesBefore, 1,
                "compatible FILE transactions share one output file");
        long acceptedMillis = TimeUnit.NANOSECONDS.toMillis(acceptedEnd - acceptedStart);
        long commitToVisibleMillis = TimeUnit.NANOSECONDS.toMillis(visibleEnd - acceptedEnd);
        double acceptedRowsPerSecond = rows * 1_000_000_000.0 / (acceptedEnd - acceptedStart);
        System.out.println(String.format(Locale.ROOT,
                "JDBC_FILE_AGGREGATION_PASS transactions=%d rowsPerTransaction=%d rows=%d "
                        + "acceptedMs=%d acceptedRowsPerSecond=%.2f commitToVisibleMs=%d files=%d",
                transactions, rowsPerTransaction, rows, acceptedMillis, acceptedRowsPerSecond,
                commitToVisibleMillis, filesAfter - filesBefore));
        return rows;
    }

    private static long awaitStableMetric(Path control, String name) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(FILE_MATERIALIZATION_DEADLINE_SECONDS);
        long value = metric(control, name);
        long stableSince = System.nanoTime();
        while (System.nanoTime() < deadline) {
            Thread.sleep(STATUS_POLL_INTERVAL_MILLIS);
            long observed = metric(control, name);
            if (observed != value) {
                value = observed;
                stableSince = System.nanoTime();
                continue;
            }
            if (System.nanoTime() - stableSince
                    >= TimeUnit.MILLISECONDS.toNanos(FILE_COUNT_STABILITY_MILLIS)) {
                return value;
            }
        }
        throw new AssertionError(name + " did not stabilize before the deadline");
    }

    private static void verifyExplicitTransaction(
            DistributedQueryRunner runner, Connection connection) throws Exception {
        equal(scalar(runner, "SELECT count(*) FROM a"), 0, "empty explicit table a");
        equal(scalar(runner, "SELECT count(*) FROM b"), 0, "empty explicit table b");

        execute(connection, "START TRANSACTION");
        update(connection, "INSERT INTO a VALUES (1, 'duplicate'), (1, 'duplicate')", 2);
        update(connection, "INSERT INTO b SELECT * FROM a", EXPLICIT_B_ROWS);
        update(connection, "INSERT INTO a SELECT * FROM a", EXPLICIT_B_ROWS);
        equal(scalar(connection, "SELECT count(*) FROM a"), EXPLICIT_A_ROWS,
                "transaction reads all completed writes to a");
        equal(scalar(connection, "SELECT count(*) FROM b"), EXPLICIT_B_ROWS,
                "transaction reads completed cross-table writes to b");
        equal(scalar(runner, "SELECT count(*) FROM a"), 0,
                "uncommitted a rows remain private");
        equal(scalar(runner, "SELECT count(*) FROM b"), 0,
                "uncommitted b rows remain private");
        execute(connection, "COMMIT");
        flushVisible(runner);

        equal(scalar(runner, "SELECT count(*) FROM a"), EXPLICIT_A_ROWS,
                "multi-statement a rows publish atomically");
        equal(scalar(runner, "SELECT count(*) FROM b"), EXPLICIT_B_ROWS,
                "multi-table b rows publish atomically");
        equal(scalar(runner,
                        "SELECT count(*) FROM a WHERE id=1 AND label='duplicate'"),
                EXPLICIT_A_ROWS,
                "same-table INSERT SELECT has no self-feedback and preserves duplicates");

        execute(connection, "START TRANSACTION");
        update(connection, "INSERT INTO a VALUES (2, 'rollback')",
                EXPLICIT_ROLLBACK_STAGED_ROWS);
        equal(scalar(connection, "SELECT count(*) FROM a"),
                EXPLICIT_A_ROWS + EXPLICIT_ROLLBACK_STAGED_ROWS,
                "transaction reads a row that will be rolled back");
        equal(scalar(runner, "SELECT count(*) FROM a"), EXPLICIT_A_ROWS,
                "rollback candidate remains private");
        execute(connection, "ROLLBACK");
        equal(scalar(runner, "SELECT count(*) FROM a"), EXPLICIT_A_ROWS,
                "rollback discards the entire transaction");
        System.out.println("JDBC_EXPLICIT_TRANSACTION_PASS aRows=" + EXPLICIT_A_ROWS
                + " bRows=" + EXPLICIT_B_ROWS);
    }

    private static void verifyFailedExplicitTransaction(
            DistributedQueryRunner runner, Connection connection) throws Exception {
        execute(connection, "START TRANSACTION");
        update(connection, "INSERT INTO a VALUES (3, 'must-not-publish')",
                EXPLICIT_FAILED_PREFIX_ROWS);
        boolean statementFailed = false;
        try {
            update(connection,
                    "INSERT INTO a SELECT n, IF(n=500, rpad('x',8192,'x'), 'pending') "
                            + "FROM UNNEST(sequence(1,500)) AS u(n)",
                    500L);
        }
        catch (SQLException expected) {
            if (!expected.getMessage().contains(
                    "One INSERT row exceeds the configured batch limit")) {
                throw expected;
            }
            statementFailed = true;
        }
        if (!statementFailed) {
            throw new AssertionError("Explicit transaction statement should fail");
        }
        boolean commitFailed = false;
        try {
            execute(connection, "COMMIT");
        }
        catch (SQLException expected) {
            commitFailed = true;
        }
        if (!commitFailed) {
            throw new AssertionError("COMMIT after a failed statement should fail");
        }
        equal(scalar(runner, "SELECT count(*) FROM a"), EXPLICIT_A_ROWS,
                "failed explicit transaction does not publish its completed prefix");
        System.out.println("JDBC_EXPLICIT_FAILURE_PASS prefixRows="
                + EXPLICIT_FAILED_PREFIX_ROWS
                + " stagedFailureRows="
                + FAILED_STATEMENT_STAGED_ROWS);
    }

    private static void verifyExplicitTransactionRejections(
            DistributedQueryRunner runner, String jdbcUrl, Properties properties) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
            execute(connection, "START TRANSACTION READ ONLY");
            expectSqlFailure(connection, "INSERT INTO a VALUES (4, 'read-only')",
                    "Cannot execute write in a read-only transaction");
            execute(connection, "ROLLBACK");
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
            execute(connection, "START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            expectSqlFailure(connection, "SELECT count(*) FROM a",
                    "does not support SERIALIZABLE isolation");
            execute(connection, "ROLLBACK");
        }
        equal(scalar(runner, "SELECT count(*) FROM a"), EXPLICIT_A_ROWS,
                "rejected transaction modes have no write side effects");
        System.out.println("JDBC_EXPLICIT_REJECTION_PASS readOnly=1 serializable=1");
    }

    private static void expectSqlFailure(
            Connection connection, String sql, String expectedMessage) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException expected) {
            if (!expected.getMessage().contains(expectedMessage)) {
                throw expected;
            }
            return;
        }
        throw new AssertionError("SQL should fail: " + sql);
    }

    private static Properties status(Path control) throws Exception {
        Properties result = new Properties();
        try (java.io.InputStream input =
                Files.newInputStream(control.resolve("status.properties"))) {
            result.load(input);
        }
        return result;
    }

    private static long metric(Path control, String name) throws Exception {
        return Long.parseLong(status(control).getProperty(name));
    }

    public static void main(String[] args) throws Exception {
        Path control = Paths.get(args[0]);
        List<URL> pluginUrls = new ArrayList<>();
        for (String path :
                Files.readString(Paths.get(args[1])).trim().split(java.io.File.pathSeparator)) {
            pluginUrls.add(Paths.get(path).toUri().toURL());
        }
        try (PluginClassLoader pluginLoader =
                        PluginManager.createClassLoader("pixels-sql-e2e", pluginUrls);
                DistributedQueryRunner runner =
                        DistributedQueryRunner.builder(
                                        testSessionBuilder()
                                                .setCatalog("pixels")
                                                .setSchema("s")
                                                .build())
                                .setWorkerCount(2)
                                .build()) {
            for (io.trino.server.testing.TestingTrinoServer server : runner.getServers()) {
                server.getInstance(com.google.inject.Key.get(HandleResolver.class))
                        .registerClassLoader(pluginLoader);
            }
            Plugin plugin =
                    (Plugin)
                            pluginLoader
                                    .loadClass("io.pixelsdb.pixels.trino.PixelsPlugin")
                                    .getConstructor()
                                    .newInstance();
            try (io.trino.spi.classloader.ThreadContextClassLoader ignored =
                    new io.trino.spi.classloader.ThreadContextClassLoader(pluginLoader)) {
                runner.installPlugin(plugin);
            }
            runner.createCatalog(
                    "pixels",
                    "pixels",
                    Map.of("cloud.function.switch", "off", "insert.enabled", "true"));
            String jdbcUrl = runner.getCoordinator().getBaseUrl().toString()
                    .replaceFirst("^http", "jdbc:trino") + "/pixels/s";
            Properties jdbcProperties = new Properties();
            jdbcProperties.setProperty("user", "pixels-e2e");
            try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties)) {
                verifyExplicitTransaction(runner, connection);
            }
            if (RUN_FAILURE_SCENARIOS) {
                try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties)) {
                    verifyFailedExplicitTransaction(runner, connection);
                }
            }
            verifyExplicitTransactionRejections(runner, jdbcUrl, jdbcProperties);
            equal(scalar(runner, "SELECT count(*) FROM t"), 0, "empty keyless table");
            insert(runner, "INSERT INTO t VALUES (1, 'a')", 1);
            equal(
                    scalar(runner, "SELECT count(*) FROM t WHERE id=1 AND label='a'"),
                    1,
                    "immediate read after commit");
            insert(runner, "INSERT INTO t VALUES (2, 'b'), (2, 'b'), (3, NULL)", 3);
            equal(
                    scalar(runner, "SELECT count(*) FROM t WHERE id=2 AND label='b'"),
                    2,
                    "duplicate values preserved");
            insert(runner, "INSERT INTO t (label, id) VALUES ('reordered', 4)", 1);
            insert(runner, "INSERT INTO t (id) VALUES (5)", 1);
            equal(
                    scalar(runner, "SELECT count(*) FROM t WHERE label IS NULL"),
                    2,
                    "omitted columns and SQL NULL");
            insert(runner, "INSERT INTO t SELECT 99, 'empty' WHERE false", 0);
            insert(
                    runner,
                    "INSERT INTO t SELECT n+1000, 'bulk' FROM UNNEST(sequence(1, 500)) AS u(n)",
                    500);
            equal(scalar(runner, "SELECT count(*) FROM t"), 506, "all bulk rows visible");
            equal(
                    scalar(runner, "SELECT sum(id) FROM t WHERE label='bulk'"),
                    625250,
                    "bulk values round trip");
            insert(
                    runner,
                    "INSERT INTO t SELECT id+100000, label FROM t WHERE id BETWEEN 1001 AND 1500",
                    500);
            equal(
                    scalar(runner, "SELECT count(*) FROM t"),
                    1006,
                    "INSERT SELECT from Pixels snapshot");
            ExecutorService writers = Executors.newFixedThreadPool(2);
            try {
                Future<?> a =
                        writers.submit(
                                () ->
                                        insert(
                                                runner,
                                                "INSERT INTO t VALUES (200000, 'concurrent-a')",
                                                1));
                Future<?> b =
                        writers.submit(
                                () ->
                                        insert(
                                                runner,
                                                "INSERT INTO t VALUES (200001, 'concurrent-b')",
                                                1));
                a.get(60, TimeUnit.SECONDS);
                b.get(60, TimeUnit.SECONDS);
            } finally {
                writers.shutdownNow();
            }
            equal(scalar(runner, "SELECT count(*) FROM t"), AUTOCOMMIT_TOTAL_ROWS,
                    "concurrent SQL commits");
            long expectedAcceptedRows = SUCCESSFUL_ACCEPTED_ROWS
                    + EXPLICIT_ROLLBACK_STAGED_ROWS
                    + (RUN_FAILURE_SCENARIOS ? EXPLICIT_FAILED_TRANSACTION_STAGED_ROWS : 0);
            long acceptedDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(SHORT_METRIC_DEADLINE_SECONDS);
            while (metric(control, "acceptedRows") < expectedAcceptedRows
                    && System.nanoTime() < acceptedDeadline) {
                Thread.sleep(STATUS_POLL_INTERVAL_MILLIS);
            }
            equal(
                    metric(control, "acceptedRows"),
                    expectedAcceptedRows,
                    "accepted rows before the failing statement");
            if (RUN_FAILURE_SCENARIOS) {
                long abortsBefore = metric(control, "abortedTransactions");
                boolean rejected = false;
                try {
                    runner.execute(
                            "INSERT INTO t SELECT n, IF(n=500, rpad('x',8192,'x'), 'pending') FROM"
                                + " UNNEST(sequence(1,500)) AS u(n)");
                } catch (RuntimeException expected) {
                    if (!expected.getMessage()
                            .contains("One INSERT row exceeds the configured batch limit")) {
                        throw new AssertionError(
                                "Unexpected failure instead of mid-stream encoder rejection", expected);
                    }
                    rejected = true;
                    System.out.println("SQL_EXPECTED_REJECTION " + expected.getMessage());
                }
                if (!rejected) {
                    throw new AssertionError("Oversized append must fail");
                }
                equal(
                        scalar(runner, "SELECT count(*) FROM t"),
                        AUTOCOMMIT_TOTAL_ROWS,
                        "no partial statement after failed append");
                long abortDeadline = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(SHORT_METRIC_DEADLINE_SECONDS);
                while (metric(control, "abortedTransactions") <= abortsBefore
                        && System.nanoTime() < abortDeadline) {
                    Thread.sleep(STATUS_POLL_INTERVAL_MILLIS);
                }
                if (metric(control, "abortedTransactions") <= abortsBefore) {
                    throw new AssertionError("Authoritative ABORT was not recorded");
                }
                if (metric(control, "acceptedRows") <= expectedAcceptedRows) {
                    throw new AssertionError("Failure test did not reach private server-side staging");
                }
            }
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(FILE_MATERIALIZATION_DEADLINE_SECONDS);
            while (metric(control, "primaryTablePixelsFiles") == 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(STATUS_POLL_INTERVAL_MILLIS);
            }
            if (metric(control, "primaryTablePixelsFiles") == 0) {
                throw new AssertionError("No Pixels files were materialized for table t");
            }
            equal(
                    scalar(runner, "SELECT count(*) FROM t"),
                    AUTOCOMMIT_TOTAL_ROWS,
                    "file and buffer handoff does not lose or duplicate rows");
            long readMetricDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(SHORT_METRIC_DEADLINE_SECONDS);
            while ((metric(control, "bufferReadRPCs") == 0
                            || metric(control, "fileVisibilityRPCs") == 0)
                    && System.nanoTime() < readMetricDeadline) {
                Thread.sleep(STATUS_POLL_INTERVAL_MILLIS);
            }
            if (metric(control, "appendRPCs") == 0
                    || metric(control, "bufferReadRPCs") == 0
                    || metric(control, "fileVisibilityRPCs") == 0) {
                throw new AssertionError(
                        "Both RPC buffer reads and physical file reads must be exercised");
            }
            long fileAggregationRows = verifyFileAggregation(runner, control);
            long finalRows = Math.addExact(AUTOCOMMIT_TOTAL_ROWS, fileAggregationRows);
            equal(scalar(runner, "SELECT count(*) FROM t"), finalRows,
                    "final rows after optional FILE aggregation workload");
            System.out.println(
                    "FULL_SQL_INSERT_E2E_PASS checks="
                            + checks
                            + " rows="
                            + finalRows
                            + " trinoWorkers=2"
                            + " appendRPCs="
                            + metric(control, "appendRPCs")
                            + " bufferReadRPCs="
                            + metric(control, "bufferReadRPCs")
                            + " fileVisibilityRPCs="
                            + metric(control, "fileVisibilityRPCs")
                            + " pixelsFiles="
                            + metric(control, "pixelsFiles")
                            + " privateAbortedRows="
                            + (metric(control, "acceptedRows")
                                    - SUCCESSFUL_ACCEPTED_ROWS
                                    - fileAggregationRows)
                            + " storage="
                            + status(control).getProperty("dataRoot"));
        }
    }
}
