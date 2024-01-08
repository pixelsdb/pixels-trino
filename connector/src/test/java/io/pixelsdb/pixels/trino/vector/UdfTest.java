package io.pixelsdb.pixels.trino.vector;

import com.google.common.collect.ImmutableMap;
import io.pixelsdb.pixels.trino.PixelsPlugin;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.Assert.assertEquals;

public class UdfTest {

    private static final QueryRunner queryRunner;

    static {
        queryRunner = createLocalQueryRunner();
        queryRunner.installPlugin(new PixelsPlugin());
    }

    @Test
    public void testEucDist() {
        MaterializedResult res1 = queryRunner.execute(createSession(), "select eucDist(array[1,2], array[1,1])");
        // Perform assertions on the result
        assertEquals(1.0, res1.getMaterializedRows().get(0).getField(0));
        MaterializedResult res2 = queryRunner.execute(createSession(), "select eucDist(array[1,2], array[2,1])");
        assertEquals(2.0, res2.getMaterializedRows().get(0).getField(0));
    }

    @Test
    public void testCosSimDist() {
        MaterializedResult res1 = queryRunner.execute(createSession(), "select cosSim(array[1,0], array[0,1])");
        // Perform assertions on the result
        assertEquals(0.0, res1.getMaterializedRows().get(0).getField(0));
        MaterializedResult res2 = queryRunner.execute(createSession(), "select cosSim(array[1,0], array[10000,0])");
        assertEquals(1.0, res2.getMaterializedRows().get(0).getField(0));
    }

    @Test
    public void testDotProdDist() {
        MaterializedResult res1 = queryRunner.execute(createSession(), "select dotProd(array[1,0], array[0,1])");
        // Perform assertions on the result
        assertEquals(0.0, res1.getMaterializedRows().get(0).getField(0));
        MaterializedResult res2 = queryRunner.execute(createSession(), "select dotProd(array[1,0], array[10000,0])");
        assertEquals(10000.0, res2.getMaterializedRows().get(0).getField(0));
    }

    /**
     * Test exactNNS using euclidean distance as distance metric
     */
    @Test
    public void testExactNNSEuc() {
        MaterializedResult res1 = queryRunner.execute(createSession(), "select exactNNS(array[1,1], 0, 'euc', 4)");
        // Perform assertions on the result
        assertEquals("[[1.1,1.1],[1.1,1.1],[0.1,0.1],[2.1,2.1]]", res1.getMaterializedRows().get(0).getField(0));

        MaterializedResult res2 = queryRunner.execute(createSession(), "select exactNNS(array[0,0], 0, 'euc', 1)");
        // Perform assertions on the result
        assertEquals("[[0.1,0.1]]", res2.getMaterializedRows().get(0).getField(0));
    }

    /**
     * Test exactNNS using dot product as distance metric
     */
    @Test
    public void testExactNNSDot() {
        MaterializedResult res1 = queryRunner.execute(createSession(), "select exactNNS(array[1,1], 0, 'dot', 1)");
        // Perform assertions on the result
        assertEquals("[[0.1,0.1]]", res1.getMaterializedRows().get(0).getField(0));

        MaterializedResult res2 = queryRunner.execute(createSession(), "select exactNNS(array[1,1], 0, 'dot', 2)");
        // Perform assertions on the result
        assertEquals("[[0.1,0.1],[0.1,0.1]]", res2.getMaterializedRows().get(0).getField(0));
    }

    /**
     * Test exactNNS using cosine similarity as distance metric
     */
    @Test
    public void testExactNNSCos() {
        MaterializedResult res1 = queryRunner.execute(createSession(), "select exactNNS(array[1,1], 0, 'cos', 1)");
        // Perform assertions on the result
        assertEquals("[[0.1,0.1]]", res1.getMaterializedRows().get(0).getField(0));
    }

    @Test
    public void testPlayGround() {
        try {
            TestTable testTable = new TestTable(createDistributedQueryRunner()::execute, "test_arr_table", "(arr_col array(double))");
        } catch (Exception e) {
            e.printStackTrace();
        }
        MaterializedResult res1 = queryRunner.execute(createSession(), "select * from test_arr_table");
        System.out.println(res1);
    }

    private static QueryRunner createLocalQueryRunner() {
        QueryRunner queryRunner = LocalQueryRunner.create(createSession());
        // Install any necessary plugins or connectors
//        queryRunner.installPlugin(new PixelsPlugin());
        return queryRunner;
    }

    public static QueryRunner createDistributedQueryRunner()
            throws Exception
    {
        // Create a Trino session
        Session session = createSession();

        // Create a distributed query runner with one node
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();

        // Install your connector plugin
        queryRunner.installPlugin(new PixelsPlugin());

//        // Create a catalog for your connector
//        queryRunner.createCatalog("your_connector_catalog", "pixels", ImmutableMap.of());

        // Return the configured QueryRunner
        return queryRunner;
    }

    private static Session createSession() {
        // Customize the session as needed
        return testSessionBuilder()
                .setCatalog("pixels")
                .setSchema("your_schema")
                .build();
    }
}
