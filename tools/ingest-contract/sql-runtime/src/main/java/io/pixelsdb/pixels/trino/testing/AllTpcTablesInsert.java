package io.pixelsdb.pixels.trino.testing;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Normal-server smoke test for every table and scalar type in TPC-H and TPC-DS. */
public final class AllTpcTablesInsert {
    private static final long DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 600;
    private static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1L);

    private record Column(String name, String type) {}

    private record Table(String name, String chunkColumn) {}

    private record Dataset(String catalog, String schema, List<Table> tables) {}

    private record Bounds(long minimum, long maximum, long nullRows) {}

    private record InsertResult(long rows, long acceptedRows, long acceptedNanos, long visibleNanos,
                                int transactions, long largestTransactionRows) {}

    private static final List<Table> TPCH_TABLES = List.of(
            new Table("customer", "custkey"),
            new Table("lineitem", "orderkey"),
            new Table("nation", "nationkey"),
            new Table("orders", "orderkey"),
            new Table("part", "partkey"),
            new Table("partsupp", "partkey"),
            new Table("region", "regionkey"),
            new Table("supplier", "suppkey"));

    private static final List<Table> TPCDS_TABLES = List.of(
            new Table("call_center", "cc_call_center_sk"),
            new Table("catalog_page", "cp_catalog_page_sk"),
            new Table("catalog_returns", "cr_order_number"),
            new Table("catalog_sales", "cs_order_number"),
            new Table("customer", "c_customer_sk"),
            new Table("customer_address", "ca_address_sk"),
            new Table("customer_demographics", "cd_demo_sk"),
            new Table("date_dim", "d_date_sk"),
            new Table("household_demographics", "hd_demo_sk"),
            new Table("income_band", "ib_income_band_sk"),
            new Table("inventory", "inv_item_sk"),
            new Table("item", "i_item_sk"),
            new Table("promotion", "p_promo_sk"),
            new Table("reason", "r_reason_sk"),
            new Table("ship_mode", "sm_ship_mode_sk"),
            new Table("store", "s_store_sk"),
            new Table("store_returns", "sr_ticket_number"),
            new Table("store_sales", "ss_ticket_number"),
            new Table("time_dim", "t_time_sk"),
            new Table("warehouse", "w_warehouse_sk"),
            new Table("web_page", "wp_web_page_sk"),
            new Table("web_returns", "wr_order_number"),
            new Table("web_sales", "ws_order_number"),
            new Table("web_site", "web_site_sk"));

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static long positiveLong(String name, long defaultValue) {
        long value = Long.parseLong(environment(name, Long.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonNegativeLong(String name, long defaultValue) {
        long value = Long.parseLong(environment(name, Long.toString(defaultValue)));
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static boolean environmentBoolean(String name, boolean defaultValue) {
        return Boolean.parseBoolean(environment(name, Boolean.toString(defaultValue)));
    }

    private static String quote(String value) {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static String qualified(String... parts) {
        return java.util.Arrays.stream(parts)
                .map(AllTpcTablesInsert::quote)
                .collect(java.util.stream.Collectors.joining("."));
    }

    private static List<Column> describe(Statement statement, String source) throws Exception {
        List<Column> columns = new ArrayList<>();
        try (ResultSet result = statement.executeQuery("DESCRIBE " + source)) {
            while (result.next()) {
                columns.add(new Column(result.getString(1), result.getString(2)));
            }
        }
        if (columns.isEmpty()) {
            throw new AssertionError("No columns for " + source);
        }
        return columns;
    }

    private static long count(Statement statement, String table) throws Exception {
        return count(statement, table, "");
    }

    private static long count(Statement statement, String table, String predicate) throws Exception {
        try (ResultSet result = statement.executeQuery(
                "SELECT count(*) FROM " + table + predicate)) {
            if (!result.next()) {
                throw new AssertionError("Missing count result for " + table);
            }
            return result.getLong(1);
        }
    }

    private static boolean tableExists(
            Statement statement, String schema, String table)
            throws Exception {
        try (ResultSet result = statement.executeQuery(
                "SELECT count(*) FROM pixels.information_schema.tables WHERE table_schema = '"
                        + schema + "' AND table_name = '" + table + "'")) {
            if (!result.next()) {
                throw new AssertionError(
                        "Missing table-existence result for " + schema + "." + table);
            }
            return result.getLong(1) == 1;
        }
    }

    private static String checksum(Statement statement, String table, List<Column> columns)
            throws Exception {
        String row = columns.stream()
                .map(Column::name)
                .map(AllTpcTablesInsert::quote)
                .collect(java.util.stream.Collectors.joining(", "));
        try (ResultSet result = statement.executeQuery(
                "SELECT to_hex(checksum(ROW(" + row + "))) FROM " + table)) {
            if (!result.next()) {
                throw new AssertionError("Missing checksum result for " + table);
            }
            return result.getString(1);
        }
    }

    private static Bounds bounds(Statement statement, String source, String chunkColumn)
            throws Exception {
        String column = quote(chunkColumn);
        try (ResultSet result = statement.executeQuery(
                "SELECT min(" + column + "), max(" + column + "), "
                        + "count_if(" + column + " IS NULL) FROM " + source)) {
            if (!result.next() || result.getObject(1) == null || result.getObject(2) == null) {
                throw new AssertionError("Missing chunk bounds for " + source);
            }
            return new Bounds(result.getLong(1), result.getLong(2), result.getLong(3));
        }
    }

    private static InsertResult insert(
            Statement statement,
            String target,
            String source,
            String chunkColumn,
            long sourceRows,
            long transactionRows,
            boolean resume,
            boolean barrierEachTransaction,
            long visibilityTimeoutSeconds)
            throws Exception {
        Bounds bounds = transactionRows == 0 || sourceRows <= transactionRows
                ? null
                : bounds(statement, source, chunkColumn);
        long nonNullRows = bounds == null ? sourceRows : sourceRows - bounds.nullRows();
        long desiredTransactions = transactionRows == 0
                ? 1
                : Math.max(1, (nonNullRows + transactionRows - 1) / transactionRows);
        long rangeWidth = bounds == null
                ? 0
                : Math.max(1, Math.addExact(
                                Math.subtractExact(bounds.maximum(), bounds.minimum()),
                                desiredTransactions)
                        / desiredTransactions);
        long rows = 0;
        long acceptedRows = 0;
        long acceptedNanos = 0;
        long visibleNanos = 0;
        long largestTransactionRows = 0;
        int transactions = 0;
        long lower = bounds == null ? 0 : bounds.minimum();
        while (bounds == null || lower <= bounds.maximum()) {
            String predicate = "";
            if (bounds != null) {
                long upper = Math.min(bounds.maximum(), Math.addExact(lower, rangeWidth - 1));
                predicate = " WHERE " + quote(chunkColumn) + " BETWEEN " + lower + " AND " + upper;
            }
            long expected = count(statement, source, predicate);
            long existing = resume ? count(statement, target, predicate) : 0;
            if (existing == expected) {
                rows += existing;
                if (bounds == null) {
                    break;
                }
                lower = Math.addExact(lower, rangeWidth);
                continue;
            }
            if (existing != 0) {
                throw new AssertionError(
                        "Partial transaction range in " + target + predicate
                                + ": expected=" + expected + ", existing=" + existing);
            }
            long acceptedStart = System.nanoTime();
            long affected = statement.executeUpdate(
                    "INSERT INTO " + target + " SELECT * FROM " + source + predicate);
            if (affected != expected) {
                throw new AssertionError(
                        "Affected rows differ from fixed source range in " + source + predicate
                                + ": expected=" + expected + ", affected=" + affected);
            }
            acceptedNanos += System.nanoTime() - acceptedStart;
            if (barrierEachTransaction) {
                visibleNanos += awaitVisible(statement, visibilityTimeoutSeconds);
            }
            rows += affected;
            acceptedRows += affected;
            largestTransactionRows = Math.max(largestTransactionRows, affected);
            transactions++;
            if (bounds == null) {
                break;
            }
            lower = Math.addExact(lower, rangeWidth);
        }
        if (bounds != null && bounds.nullRows() > 0) {
            String predicate = " WHERE " + quote(chunkColumn) + " IS NULL";
            long existing = resume ? count(statement, target, predicate) : 0;
            if (existing == bounds.nullRows()) {
                rows += existing;
            }
            else if (existing != 0) {
                throw new AssertionError(
                        "Partial null-key transaction in " + target
                                + ": expected=" + bounds.nullRows() + ", existing=" + existing);
            }
            else {
                long acceptedStart = System.nanoTime();
                long affected = statement.executeUpdate(
                        "INSERT INTO " + target + " SELECT * FROM " + source + predicate);
                if (affected != bounds.nullRows()) {
                    throw new AssertionError(
                            "Affected rows differ from null-key source range in " + source
                                    + ": expected=" + bounds.nullRows() + ", affected=" + affected);
                }
                acceptedNanos += System.nanoTime() - acceptedStart;
                if (barrierEachTransaction) {
                    visibleNanos += awaitVisible(statement, visibilityTimeoutSeconds);
                }
                rows += affected;
                acceptedRows += affected;
                largestTransactionRows = Math.max(largestTransactionRows, affected);
                transactions++;
            }
        }
        if (!barrierEachTransaction && transactions > 0) {
            visibleNanos += awaitVisible(statement, visibilityTimeoutSeconds);
        }
        return new InsertResult(
                rows, acceptedRows, acceptedNanos, visibleNanos,
                transactions, largestTransactionRows);
    }

    private static long awaitVisible(Statement statement, long visibilityTimeoutSeconds)
            throws Exception {
        long start = System.nanoTime();
        statement.execute(
                "CALL pixels.system.flush_visible_barrier(timeout_seconds => "
                        + visibilityTimeoutSeconds + ")");
        return System.nanoTime() - start;
    }

    private static long copy(
            Statement statement,
            Dataset dataset,
            String targetSchema,
            Path dataRoot,
            Table table,
            boolean verifyOnly,
            boolean resumeExisting,
            boolean barrierEachTransaction,
            long visibilityTimeoutSeconds,
            long transactionRows)
            throws Exception {
        String source = qualified(dataset.catalog(), dataset.schema(), table.name());
        String targetName = dataset.catalog() + "_" + table.name();
        String target = qualified("pixels", targetSchema, targetName);
        List<Column> columns = describe(statement, source);
        long sourceScanStart = System.nanoTime();
        long sourceRows = count(statement, source);
        String sourceChecksum = checksum(statement, source, columns);
        long sourceScanNanos = System.nanoTime() - sourceScanStart;
        boolean completedBeforeRun = false;
        InsertResult inserted = new InsertResult(sourceRows, 0, 0, 0, 0, 0);
        if (!verifyOnly) {
            String definitions = columns.stream()
                    .map(column -> quote(column.name()) + " " + column.type())
                    .collect(java.util.stream.Collectors.joining(", "));
            String path = dataRoot.resolve(targetSchema).resolve(targetName).toUri().toString();
            if (!resumeExisting) {
                statement.execute(
                        "CREATE TABLE " + target + " (" + definitions
                                + ") WITH (storage='file', paths='"
                                + path.replace("'", "''") + "')");
            }
            else {
                long existingRows = count(statement, target);
                if (existingRows == sourceRows
                        && sourceChecksum.equals(checksum(statement, target, columns))) {
                    completedBeforeRun = true;
                }
            }
            if (!completedBeforeRun) {
                inserted = insert(
                        statement,
                        target,
                        source,
                        table.chunkColumn(),
                        sourceRows,
                        transactionRows,
                        resumeExisting,
                        barrierEachTransaction,
                        visibilityTimeoutSeconds);
            }
        }
        long targetRows = count(statement, target);
        String targetChecksum = checksum(statement, target, columns);
        if (inserted.rows() != sourceRows || targetRows != sourceRows
                || !sourceChecksum.equals(targetChecksum)) {
            throw new AssertionError(
                    source + " mismatch: sourceRows=" + sourceRows + ", affected=" + inserted.rows()
                            + ", targetRows=" + targetRows + ", sourceChecksum=" + sourceChecksum
                            + ", targetChecksum=" + targetChecksum);
        }
        System.out.println(String.format(
                Locale.ROOT,
                "TPC_TABLE_%s_PASS source=%s rows=%d columns=%d sourceScanMs=%d "
                        + "acceptedRows=%d acceptedMs=%d acceptedRowsPerSecond=%.2f "
                        + "visibleAfterAcceptedMs=%d "
                        + "transactions=%d largestTransactionRows=%d "
                        + "checksum=%s",
                verifyOnly || completedBeforeRun ? "RECOVERY" : "INSERT",
                source,
                sourceRows,
                columns.size(),
                TimeUnit.NANOSECONDS.toMillis(sourceScanNanos),
                inserted.acceptedRows(),
                TimeUnit.NANOSECONDS.toMillis(inserted.acceptedNanos()),
                inserted.acceptedNanos() == 0
                        ? 0
                        : inserted.acceptedRows() * (double) NANOSECONDS_PER_SECOND
                                / inserted.acceptedNanos(),
                TimeUnit.NANOSECONDS.toMillis(inserted.visibleNanos()),
                inserted.transactions(),
                inserted.largestTransactionRows(),
                sourceChecksum));
        return sourceRows;
    }

    private static String requiredIdentifier(String value, String description) {
        if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(description + " is not a SQL identifier: " + value);
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            throw new IllegalArgumentException(
                    "Usage: AllTpcTablesInsert <jdbc-url> <data-root> [target-schema] [user]");
        }
        String targetSchema = requiredIdentifier(
                args.length >= 3 ? args[2] : "tpc_insert_all", "target schema");
        String user = args.length == 4 ? args[3] : System.getProperty("user.name", "pixels");
        Path dataRoot = Path.of(args[1]).toAbsolutePath().normalize();
        boolean verifyOnly = Boolean.parseBoolean(
                System.getenv().getOrDefault("ALL_TPC_VERIFY_ONLY", "false"));
        boolean resume = Boolean.parseBoolean(
                System.getenv().getOrDefault("ALL_TPC_RESUME", "false"));
        String tpchSchema = requiredIdentifier(
                environment("TPCH_SCHEMA", "tiny"), "TPC-H schema");
        String tpcdsSchema = requiredIdentifier(
                environment("TPCDS_SCHEMA", "tiny"), "TPC-DS schema");
        long visibilityTimeoutSeconds = positiveLong(
                "ALL_TPC_VISIBILITY_TIMEOUT_SECONDS", DEFAULT_VISIBILITY_TIMEOUT_SECONDS);
        long transactionRows = nonNegativeLong("ALL_TPC_TRANSACTION_ROWS", 0);
        boolean barrierEachTransaction = environmentBoolean(
                "ALL_TPC_BARRIER_EACH_TRANSACTION", true);
        List<Dataset> datasets = List.of(
                new Dataset("tpch", tpchSchema, TPCH_TABLES),
                new Dataset("tpcds", tpcdsSchema, TPCDS_TABLES));
        long rows = 0;
        int tables = 0;
        try (Connection connection = DriverManager.getConnection(args[0], user, null);
                Statement statement = connection.createStatement()) {
            if (!verifyOnly && resume) {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + qualified("pixels", targetSchema));
            }
            else if (!verifyOnly) {
                statement.execute("CREATE SCHEMA " + qualified("pixels", targetSchema));
            }
            for (Dataset dataset : datasets) {
                for (Table table : dataset.tables()) {
                    boolean targetExists = tableExists(
                            statement,
                            targetSchema,
                            dataset.catalog() + "_" + table.name());
                    rows += copy(
                            statement,
                            dataset,
                            targetSchema,
                            dataRoot,
                            table,
                            verifyOnly,
                            resume && targetExists,
                            barrierEachTransaction,
                            visibilityTimeoutSeconds,
                            transactionRows);
                    tables++;
                }
            }
        }
        System.out.println(String.format(
                Locale.ROOT,
                "ALL_TPC_TABLES_%s_PASS tables=%d rows=%d targetSchema=%s dataRoot=%s",
                verifyOnly ? "RECOVERY" : "INSERT",
                tables,
                rows,
                targetSchema,
                dataRoot));
    }
}
