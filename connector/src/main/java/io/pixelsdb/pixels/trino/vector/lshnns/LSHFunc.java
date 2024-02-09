package io.pixelsdb.pixels.trino.vector.lshnns;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.BitSet;

public class LSHFunc {

    @JsonProperty
    RealMatrix planeNorms;
    @JsonProperty
    int dimension;
    @JsonProperty
    int numBits;

    public LSHFunc(int dimension, int nBits, long seed) {
        RandomGenerator randomGenerator = new JDKRandomGenerator();
        randomGenerator.setSeed(seed);

        // Create a random matrix
        planeNorms = generateRandomMatrix(nBits, dimension, randomGenerator);
        this.dimension = dimension;
        this.numBits = nBits;
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
    public static class RealMatrixSerializer extends StdSerializer<RealMatrix> {
        public RealMatrixSerializer() {
            this(null);
        }

        public RealMatrixSerializer(Class<RealMatrix> t) {
            super(t);
        }

        @Override
        public void serialize(RealMatrix value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            // Convert RealMatrix to a 2D array or as a list of lists
            gen.writeObject(value.getData());
        }
    }

    /** deserializer for the field RealMatrix planeNorms */
    public static class RealMatrixDeserializer extends StdDeserializer<RealMatrix> {
        public RealMatrixDeserializer() {
            this(null);
        }

        public RealMatrixDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public RealMatrix deserialize(com.fasterxml.jackson.core.JsonParser parser, com.fasterxml.jackson.databind.DeserializationContext ctxt) throws IOException, com.fasterxml.jackson.core.JsonProcessingException {
            double[][] arr = parseMatrixData(parser.readValueAsTree());
            // Deserialize JSON into a RealMatrix object
            // For example, you can deserialize it from a 2D array or from a list of lists
            // Example: return new Array2DRowRealMatrix(p.readValueAs(double[][].class));
            return new Array2DRowRealMatrix(arr);
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
