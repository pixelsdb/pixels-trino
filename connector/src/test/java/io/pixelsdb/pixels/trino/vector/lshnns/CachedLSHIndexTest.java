package io.pixelsdb.pixels.trino.vector.lshnns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;
import io.trino.spi.type.ArrayType;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.IOException;
import java.util.HashMap;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.junit.Assert.*;

public class CachedLSHIndexTest {
    static ObjectMapper objectMapper;

    @BeforeClass
    public static void setUp() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(RealMatrix.class, new LSHFunc.RealMatrixSerializer());
        module.addDeserializer(RealMatrix.class, new LSHFunc.RealMatrixDeserializer());
        objectMapper = new ObjectMapper().registerModule(module);
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
        CachedLSHIndex.updateColToBuckets(pixelsColumnHandle, "s3dir", new LSHFunc(2, 4, 42));
    }

    @Test
    public void testSerialization() {
        // todo add an entry to the colToBuckets index
        try {
            HashMap<PixelsColumnHandle, CachedLSHIndex.Buckets> colToBuckets1 = CachedLSHIndex.getColToBuckets();
            byte[] bytes = objectMapper.writeValueAsBytes(colToBuckets1);
            HashMap<PixelsColumnHandle, CachedLSHIndex.Buckets> colToBuckets2 = objectMapper.readValue(bytes, HashMap.class);
            System.out.println(colToBuckets2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteColToBucketsToS3() {
        String s3Bucket = "tiannan-test";
        String s3Key = "LSHIndex/LSHIndex.json";
        CachedLSHIndex.writeColToBucketsToS3(s3Bucket, s3Key);
    }

    @Test
    public void testInitLshIndexFomS3() {
        CachedLSHIndex.initLshIndexFomS3();
        HashMap<PixelsColumnHandle, CachedLSHIndex.Buckets> colToBuckets = CachedLSHIndex.getColToBuckets();
        System.out.println(colToBuckets);
    }
}