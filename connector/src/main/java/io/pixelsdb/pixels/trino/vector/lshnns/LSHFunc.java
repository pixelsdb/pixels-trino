package io.pixelsdb.pixels.trino.vector.lshnns;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.htrace.shaded.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Objects;


@JsonSerialize(using = LSHFunc.LSHFuncSerializer.class)
@JsonDeserialize(using = LSHFunc.LSHFuncDeserializer.class)
public class LSHFunc {

    @JsonProperty
    RealMatrix planeNorms;
    @JsonProperty
    int dimension;
    @JsonProperty
    int numBits;

    public LSHFunc() {}

    public LSHFunc(int dimension, int numBits, long seed) {
        RandomGenerator randomGenerator = new JDKRandomGenerator();
        randomGenerator.setSeed(seed);

        // Create a random matrix
        planeNorms = generateRandomMatrix(numBits, dimension, randomGenerator);
        this.dimension = dimension;
        this.numBits = numBits;
    }

    public LSHFunc(int dimension, int numBits, RealMatrix planeNorms) {
        this.dimension = dimension;
        this.numBits = numBits;
        this.planeNorms = planeNorms;
    }

    // Utility method to generate a random matrix
    private static RealMatrix generateRandomMatrix(int rows, int cols, RandomGenerator randomGenerator) {
        double[][] matrixData = new double[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                // subtract 0.5 to move the interval from [0.0, 1.0] to [-0.5, 0.5]
                matrixData[i][j] = randomGenerator.nextDouble() - 0.5;
            }
        }
        return new Array2DRowRealMatrix(matrixData);
    }

    public BitSet hash(double[] vector) {
        BitSet bitSet = new BitSet(numBits);
        double[] dotProducts =  planeNorms.operate(vector);
        for (int i=0; i<dotProducts.length; i++) {
            if (dotProducts[i] > 0) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    @Override
    public String toString() {
        return "LSHFunc{" +
                "dimension" + dimension +
                "numBits" + numBits +
                "plane_norms=" + planeNorms +
                '}';
    }

    public int getNumBits() {
        return numBits;
    }

    public int getDimension() {
        return dimension;
    }

    public RealMatrix getPlaneNorms() {
        return planeNorms;
    }

    /** serializer for the field RealMatrix planeNorms */
    public static class LSHFuncSerializer extends JsonSerializer<LSHFunc> {

        @Override
        public void serialize(LSHFunc value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("planeNorms");
            gen.writeObject(value.planeNorms.getData());
            gen.writeNumberField("dimension", value.getDimension());
            gen.writeNumberField("numBits", value.getNumBits());
            gen.writeEndObject();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LSHFunc lshFunc = (LSHFunc) o;
        return dimension == lshFunc.dimension && numBits == lshFunc.numBits; //&& planeNorms.equals(lshFunc.planeNorms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planeNorms, dimension, numBits);
    }

    public static String hashKeyToString(BitSet bitSet) {
        StringBuilder res = new StringBuilder();
        for (long num : bitSet.toLongArray()) {
            res.append(num).append("_");
        }
        return res.toString();
    }

    /** deserializer for the field RealMatrix planeNorms */
    public static class LSHFuncDeserializer extends JsonDeserializer<LSHFunc> {

        @Override
        public LSHFunc deserialize(com.fasterxml.jackson.core.JsonParser parser, com.fasterxml.jackson.databind.DeserializationContext ctxt) throws IOException {

            JsonNode node = parser.getCodec().readTree(parser);
            JsonNode planeNormsNode = node.get("planeNorms");
            double[][] matrixArr = parseMatrixData(planeNormsNode);
            int dimension = node.get("dimension").asInt();
            int numBits = node.get("numBits").asInt();
            return new LSHFunc(dimension, numBits, new Array2DRowRealMatrix(matrixArr));
        }

        private double[][] parseMatrixData(JsonNode node) {
            int numRows = node.size();
            int numCols = node.get(0).size();
            double[][] matrixData = new double[numRows][numCols];
            for (int i = 0; i < numRows; i++) {
                JsonNode row = node.get(i);
                for (int j = 0; j < numCols; j++) {
                    matrixData[i][j] = row.get(j).asDouble();
                }
            }
            return matrixData;
        }
    }
}
