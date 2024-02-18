package io.pixelsdb.pixels.trino.vector.lshnns;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.type.ArrayType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.IOException;
import java.util.HashMap;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class CachedLSHIndexTest {
    static ObjectMapper objectMapper;

    @BeforeClass
    public static void setUp() {
        System.setProperty(CachedLSHIndex.RUNNING_UNIT_TESTS, "true");
        objectMapper = new ObjectMapper();
        PixelsColumnHandle pixelsColumnHandle = PixelsColumnHandle.builder()
                .setConnectorId("abcdef")
                .setSchemaName("pixels")
                .setColumnName("arr_col")
                .setColumnAlias("alias")
                .setColumnType(new ArrayType(DOUBLE))
                .setTypeCategory(TypeDescription.Category.VECTOR)
                .setLogicalOrdinal(0)
                .setColumnComment("")
                .setTableName("test_table").build();
        CachedLSHIndex.getInstance().setCurrColumn(pixelsColumnHandle);
        CachedLSHIndex.getInstance().updateColToBuckets(pixelsColumnHandle, "s3dir", new LSHFunc(2, 4, 42));
    }

    @Test
    public void testSerialization() {
        // todo add an entry to the colToBuckets index
        try {
            PixelsColumnHandle pixelsColumnHandle2 = PixelsColumnHandle.builder()
                    .setConnectorId("abcdef")
                    .setSchemaName("pixels")
                    .setColumnName("arr_col")
                    .setColumnAlias("alias")
                    .setColumnType(new ArrayType(DOUBLE))
                    .setTypeCategory(TypeDescription.Category.VECTOR)
                    .setLogicalOrdinal(0)
                    .setColumnComment("")
                    .setTableName("test_table").build();
            HashMap<String, CachedLSHIndex.Buckets> map = CachedLSHIndex.getInstance().getColToBuckets();
            System.out.println(map);
            byte[] bytes = objectMapper.writeValueAsBytes(map);
            HashMap<String, CachedLSHIndex.Buckets> map2 = objectMapper.readValue(bytes, new TypeReference<>() {});
            System.out.println("map2");
            System.out.println(map2.get(CachedLSHIndex.getInstance().getCurrColAsStr()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // todo maybe create a new class Column, which is simplified columnHandle
    }

    @Test
    public void testSerializationLSHFunc() {
        try {
            LSHFunc lshFunc = new LSHFunc(2,4, 42);
            byte[] lshFuncBytes = objectMapper.writeValueAsBytes(lshFunc);
            LSHFunc lshFunc2 = objectMapper.readValue(lshFuncBytes, LSHFunc.class);
            assert(lshFunc.equals(lshFunc2));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSerializationBuckets() {
        try {
            LSHFunc lshFunc = new LSHFunc(2,4, 42);
            CachedLSHIndex.Buckets buckets = new CachedLSHIndex.Buckets("s3dir", lshFunc);
            System.out.println("buckets: " + buckets);
            byte[] bytes = objectMapper.writeValueAsBytes(buckets);
            CachedLSHIndex.Buckets buckets2 = objectMapper.readValue(bytes, CachedLSHIndex.Buckets.class);
            System.out.println("buckets2.tableS3Path: " + buckets2.tableS3Path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSerializePixelsColumnHandle() {
        try {
            PixelsColumnHandle pixelsColumnHandle = PixelsColumnHandle.builder()
                    .setConnectorId("abcdef")
                    .setSchemaName("pixels")
                    .setColumnName("arr_col")
                    .setColumnAlias("alias")
                    .setColumnType(new ArrayType(DOUBLE))
                    .setTypeCategory(TypeDescription.Category.VECTOR)
                    .setLogicalOrdinal(0)
                    .setColumnComment("")
                    .setTableName("test_table").build();
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] pchBytes = objectMapper.writeValueAsBytes(pixelsColumnHandle);
            PixelsColumnHandle pixelsColumnHandle2 = objectMapper.readValue(pchBytes, PixelsColumnHandle.class);
            assert(pixelsColumnHandle.equals(pixelsColumnHandle2));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteColToBucketsToS3() {
        String s3Bucket = "tiannan-test";
        String s3Key = "LSHIndex/LSHIndex.json";
        CachedLSHIndex.getInstance().writeColToBucketsToS3(s3Bucket, s3Key);
    }

    @Test
    public void testInitLshIndexFomS3() {
        CachedLSHIndex.getInstance().loadLshIndexFomS3();
        HashMap<String, CachedLSHIndex.Buckets> colToBuckets = CachedLSHIndex.getInstance().getColToBuckets();
        System.out.println(colToBuckets);
    }

    @AfterClass
    public static void cleanUp() {
        System.setProperty("runningUnitTests", "false");
    }
}