package io.pixelsdb.pixels.trino.vector.lshnns;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.pixelsdb.pixels.trino.PixelsColumnHandle;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class CachedLSHIndex {

    public static final String S3_BUCKET = "tiannan-test";
    public static final String S3_KEY = "LSHIndex/LSHIndex.json";
    public static final String RUNNING_UNIT_TESTS = "runningUnitTests";

    // the column queried by current query
    PixelsColumnHandle currColumn;

    // map each LSH indexed column to the corresponding LSH buckets
    @JsonSerialize(using = ColToBucketsSerializer.class)
    @JsonDeserialize(using = ColToBucketsDeserializer.class)
    private HashMap<String, Buckets> colToBuckets;

    static ObjectMapper objectMapper;

    private S3Client s3;
    //private static boolean initialized = false;

    private static CachedLSHIndex instance;

    private CachedLSHIndex() {
//        SimpleModule module = new SimpleModule();
//        module.addSerializer(RealMatrix.class, new LSHFunc.RealMatrixSerializer());
//        module.addDeserializer(RealMatrix.class, new LSHFunc.RealMatrixDeserializer());
//        objectMapper = new ObjectMapper().registerModule(module);
        objectMapper = new ObjectMapper();

        s3 = S3Client.builder()
                .region(Region.US_EAST_2)
                .build();
        loadLshIndexFomS3();

        // make sure that we write the lsh index to s3 file right before jvm is shut down
        Runtime.getRuntime().addShutdownHook(
                new Thread(()->writeColToBucketsToS3(S3_BUCKET, S3_KEY))
        );
        //initialized = true;
    }

    public static synchronized CachedLSHIndex getInstance() {
        if (instance == null) {
            instance = new CachedLSHIndex();
        }
        return instance;
    }

    void loadLshIndexFomS3() {
        if (colToBuckets != null) {
            // already initialized
            return;
        }
        // load the lsh index file from s3
        byte[] bytes = getLSHIndexFromS3(S3_BUCKET, S3_KEY);
        if (bytes==null) {
            colToBuckets = new HashMap<>();
            return;
        }
        try {
            // deserialize JSON data from S3 object
            colToBuckets = objectMapper.readValue(bytes, new TypeReference<>() {
            });
        } catch (IOException e) {
            //e.printStackTrace();
            colToBuckets = new HashMap<>();
        }
    }

    void writeColToBucketsToS3(String bucket, String key) {
        // avoid flushing index to s3 if we are running tests
        String isTesting = System.getProperty(RUNNING_UNIT_TESTS);
        if (isTesting != null && isTesting.toLowerCase().equals("true")) {
            return;
        }
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

    public HashMap<String, Buckets> getColToBuckets() {
        return colToBuckets;
    }

    public PixelsColumnHandle getCurrColumn() {
        return currColumn;
    }

    public void setCurrColumn(PixelsColumnHandle currColumn) {
        this.currColumn = currColumn;
    }

    public void updateColToBuckets(String tableS3Path, LSHFunc lshFunc) {
        // when executing an LSH build query, the currColumn will be updated earlier in PixelsPageSourceProvider
        colToBuckets.put(getCurrColAsStr(), new Buckets(tableS3Path, lshFunc));
    }

    public void updateColToBuckets(PixelsColumnHandle col, String tableS3Path, LSHFunc lshFunc) {
        // when executing an LSH build query, the currColumn will be updated earlier in SplitManager
        colToBuckets.put(getCurrColAsStr(), new Buckets(tableS3Path, lshFunc));
    }

    public void updateColToBuckets(String schemaTableCol, String tableS3Path, LSHFunc lshFunc) {
        // when executing an LSH build query, the currColumn will be updated earlier in SplitManager
        colToBuckets.put(schemaTableCol, new Buckets(tableS3Path, lshFunc));
    }

    public String getCurrColAsStr() {
        return currColumn.getSchemaName() + "." + currColumn.getTableName() + "." + currColumn.getColumnName();
    }



    public byte[] getLSHIndexFromS3(String bucketName, String keyName) {
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
     * get the LSH buckets for currColumn. Buckets consist of the S3 path storing the buckets and the lsh function used for currColumn
     * @return the LSH buckets for current column
     */
    public Buckets getBuckets() {
        return colToBuckets.get(getCurrColAsStr());
    }

    /**
     * class represents LSH buckets for a column, which includes an LSH function and the S3 paths for
     */
    @JsonSerialize(using = BucketsSerializer.class)
    @JsonDeserialize(using = BucketsDeserializer.class)
    public static class Buckets {
        String tableS3Path;
        LSHFunc lshFunc;

        Buckets() {

        }

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

    public static class BucketsDeserializer extends JsonDeserializer<Buckets> {

        @Override
        public Buckets deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JacksonException {
            ObjectMapper objectMapper = (ObjectMapper) jp.getCodec();
            JsonNode node = jp.getCodec().readTree(jp);
            String tableS3Path = node.get("tableS3Path").asText();

            JsonNode lshFuncNode = node.get("lshFunc");
            LSHFunc lshFunc = objectMapper.treeToValue(lshFuncNode, LSHFunc.class); // Deserialize B using its deserializer

            return new Buckets(tableS3Path, lshFunc);
        }
    }

    public static class BucketsSerializer extends JsonSerializer<Buckets> {

        @Override
        public void serialize(Buckets buckets, JsonGenerator gen, SerializerProvider serializerProvider) throws IOException {
            gen.writeStartObject();
            gen.writeStringField("tableS3Path", buckets.tableS3Path);
            gen.writeFieldName("lshFunc");
            serializerProvider.defaultSerializeValue(buckets.getLshFunc(), gen);
            gen.writeEndObject();
        }
    }

    public static class ColToBucketsSerializer extends JsonSerializer<HashMap<String, Buckets>> {

        @Override
        public void serialize(HashMap<String, Buckets> map, JsonGenerator gen, SerializerProvider serializerProvider) throws IOException {
            gen.writeStartObject();
            for (String key : map.keySet()) {
                gen.writeFieldName(key);
                Buckets buckets = map.get(key);
                // Use the customized serializer for Buckets
                serializerProvider.defaultSerializeValue(buckets, gen);
            }
            gen.writeEndObject();
        }
    }

    public static class ColToBucketsDeserializer extends JsonDeserializer<HashMap<String, Buckets>> {

        @Override
        public HashMap<String, Buckets> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
            HashMap<String, Buckets> res = new HashMap<>();
            ObjectMapper objectMapper = (ObjectMapper) jsonParser.getCodec();
            JsonNode root = objectMapper.readTree(jsonParser);
            Iterator<String> colIter = root.fieldNames();
            while (colIter.hasNext()) {
                String column = colIter.next();
                JsonNode bucketsNode = root.get(column);
                Buckets buckets = objectMapper.treeToValue(bucketsNode, Buckets.class);
                res.put(column, buckets);
            }
            return res;
        }
    }
}
