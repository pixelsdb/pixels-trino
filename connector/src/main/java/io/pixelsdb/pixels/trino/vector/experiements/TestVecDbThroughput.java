package io.pixelsdb.pixels.trino.vector.experiements;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/** This class is put here for the purpose of grading my master thesis
 *  The real usage of this class is in a separate repo which can be packed into an executable shaded jar.
 */
public class TestVecDbThroughput {
    static Random random = new Random(42);
    static double[][] inputVecs;
    static int numQueries = 5;
    static String queryType = "lsh";

    public static void main(String[] args) {
        if (args.length > 0) {
            queryType = args[0];
        }
        if (args.length == 2) {
            numQueries = Integer.parseInt(args[1]);
        }

        inputVecs = new double[numQueries][300];
        for (int i=0; i<numQueries; i++) {
            for (int j=0; j<300; j++) {
                inputVecs[i][j] = random.nextDouble();
            }
        }

        try {
            Class.forName("io.trino.jdbc.TrinoDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // JDBC connection parameters
        String jdbcUrl = "jdbc:trino://localhost:8080/pixels/test_schema?user=pixels&SSL=false";

        try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            runLSHQueryTestLatency("embd_1m_2048buckets", connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void runLSHQueryTestLatency(String table, Connection connection) {


        ExecutorService executorService = Executors.newFixedThreadPool(2);
        List<Future<ResultSet>> futures = new ArrayList<>(numQueries);

        long start = System.currentTimeMillis();
        for (int i=0; i<numQueries; i++) {
            try {
                String query = getQuery(inputVecs[i]);
                System.out.println(query);
                PreparedStatement statement = connection.prepareStatement(query);
                futures.add(executorService.submit((Callable<ResultSet>) statement::executeQuery));

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            for (Future<ResultSet> future : futures) {
                ResultSet resultSet = future.get();
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        } catch (SQLException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(180, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("query on table " + table + " has QPS = " + numQueries / ((end - start) / 1000.0));
    }

    static String getQuery(double[] vec) {
        if (queryType.equalsIgnoreCase("lsh")) {
            return String.format("select lsh_search('%s', 'euc', 'test_schema.embd_1m_2048buckets.embd_col',5)", Arrays.toString(vec));
        } else if (queryType.equalsIgnoreCase("exactnns")) {
            return String.format("select exact_nns(embd_col, '%s', 'euc', 5) from embd_1m_table", Arrays.toString(vec));
        } else {
            System.out.println("Query type should be either lsh or exactnns. Unknown query type: " + queryType);
            return null;
        }
    }
}
