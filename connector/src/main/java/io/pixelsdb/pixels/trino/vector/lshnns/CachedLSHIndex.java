package io.pixelsdb.pixels.trino.vector.lshnns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.math3.linear.RealMatrix;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class CachedLSHIndex {

    public static final String s3Bucket = "tiannan-test";
    public static final String s3Key = "LSHIndex/LSHIndex.json";

    // the column queried by current query
    static PixelsColumnHandle currColumn;

    // map each LSH indexed column to the corresponding LSH buckets
    private static volatile HashMap<PixelsColumnHandle, Buckets> colToBuckets;

    static ObjectMapper objectMapper;
    //static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    private static S3Client s3;

    // initialize the LSH index. If not already loaded, we load the LSH index from a file
    static {
        SimpleModule module = new SimpleModule();
        module.addSerializer(RealMatrix.class, new LSHFunc.RealMatrixSerializer());
        module.addDeserializer(RealMatrix.class, new LSHFunc.RealMatrixDeserializer());
        objectMapper = new ObjectMapper().registerModule(module);

        s3 = S3Client.builder()
                .region(Region.US_EAST_2)
                .build();
        initLshIndexFomS3();

        // make sure that we write the lsh index to s3 file right before jvm is shut down
        Runtime.getRuntime().addShutdownHook(
                new Thread(()->writeColToBucketsToS3(s3Bucket, s3Key))
        );
    }

    static void initLshIndexFomS3() {
        if (colToBuckets != null) {
            // already initialized
            return;
        }
        // load the lsh index file from s3
        byte[] bytes = getLSHIndexFromS3(s3Bucket, s3Key);
        if (bytes==null) {
            colToBuckets = new HashMap<>();
            return;
        }
        try {
            // deserialize JSON data from S3 object
            colToBuckets = objectMapper.readValue(bytes, HashMap.class);
        } catch (IOException e) {
            //e.printStackTrace();
            colToBuckets = new HashMap<>();
        }
    }

    static void writeColToBucketsToS3(String bucket, String key) {
        if (colToBuckets==null) {
            return;
        }
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(colToBuckets);
            PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(key).build();
            s3.putObject(putObjectRequest, RequestBody.fromBytes(bytes));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public static HashMap<PixelsColumnHandle, Buckets> getColToBuckets() {
        return colToBuckets;
    }

    public static PixelsColumnHandle getCurrColumn() {
        return currColumn;
    }

    public static void setCurrColumn(PixelsColumnHandle currColumn) {
        CachedLSHIndex.currColumn = currColumn;
    }

    public static void updateColToBuckets(String tableS3Path, LSHFunc lshFunc) {
        // when executing an LSH build query, the currColumn will be updated earlier in SplitManager
        colToBuckets.put(currColumn, new Buckets(tableS3Path, lshFunc));
    }

    static void updateColToBuckets(PixelsColumnHandle col, String tableS3Path, LSHFunc lshFunc) {
        // when executing an LSH build query, the currColumn will be updated earlier in SplitManager
        colToBuckets.put(col, new Buckets(tableS3Path, lshFunc));
    }

    public static byte[] getLSHIndexFromS3(String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();
            return s3.getObjectAsBytes(objectRequest).asByteArray();
        } catch (S3Exception e) {
            //e.printStackTrace();
            return null;
        }
    }


    /**
     * get the LSH buckets for currColumn. Buckets consist of the S3 path storing the buckets and the lsh function used
     * for currColumn
     * @return the LSH buckets for current column
     */
    public static Buckets getBuckets() {
        return colToBuckets.get(currColumn);
    }

    public static class Buckets {
        String tableS3Path;
        LSHFunc lshFunc;

        Buckets(String tableS3Path, LSHFunc lshFunc) {
            this.tableS3Path = tableS3Path;
            this.lshFunc = lshFunc;
        }

        public LSHFunc getLshFunc() {
            return lshFunc;
        }

        public String getTableS3Path() {
            return tableS3Path;
        }

        @Override
        public String toString() {
            return "Buckets{" +
                    "tableS3Path='" + tableS3Path + '\'' +
                    ", lshFunc=" + lshFunc +
                    '}';
        }
    }
}
