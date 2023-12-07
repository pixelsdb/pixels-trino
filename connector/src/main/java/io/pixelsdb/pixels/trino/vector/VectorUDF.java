package io.pixelsdb.pixels.trino.vector;

import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class VectorUDF {

    private VectorUDF() {}

    @ScalarFunction("exactNNS")
    @Description("exact nearest neighbours search")
    @SqlType(StandardTypes.DOUBLE)
    public static double exactNNS(
            @SqlNullable @SqlType("array(double)") Block vector)
    {
        // for now just retreive each element of the vector

        ArrayList<Double> features = new ArrayList<>();

        if (vector != null) {
            for (int position = 0; position < vector.getPositionCount(); position++) {
                features.add(DOUBLE.getDouble(vector, position));
            }
        }

        double sum = 0.0;
        for (double v : features) {
            sum += v;
        }
        return sum;
    }

    @ScalarFunction("eucDist")
    @Description("calculate the Euclidean distance between two vectors")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double eucDist(
            @SqlNullable @SqlType("array(double)") Block vec1,
            @SqlNullable @SqlType("array(double)") Block vec2)
    {
        if (!distIsDefined(vec1, vec2)) {
            return null;
        }

        double dist = 0.0;
        for (int position = 0; position < vec1.getPositionCount(); position++) {
            //todo can also use multi threads and let different threads be responsible for different elements
            // one thread for calculating (x[1]-y[1])^2, another (x[2]-y[2))^2
            // let's keep it simple and only use single thread for now
            double xi = DOUBLE.getDouble(vec1, position);
            double yi = DOUBLE.getDouble(vec2, position);
            dist += (xi-yi)*(xi-yi);
        }
        return dist;
    }

    @ScalarFunction("dotProd")
    @Description("calculate the dot product between two vectors")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double dotProd(
            @SqlNullable @SqlType("array(double)") Block vec1,
            @SqlNullable @SqlType("array(double)") Block vec2)
    {
        if (!distIsDefined(vec1, vec2)) {
            return null;
        }

        double dist = 0.0;
        for (int position = 0; position < vec1.getPositionCount(); position++) {
            //todo can also use multi threads and let different threads be responsible for different elements
            // one thread for calculating x[1]*y[1], another x[2]*y[2]
            // let's keep it simple and only use single thread for now
            double xi = DOUBLE.getDouble(vec1, position);
            double yi = DOUBLE.getDouble(vec2, position);
            dist += xi*yi;
        }
        return dist;
    }

    @ScalarFunction("cosSim")
    @Description("calculate the cosine similarity between two vectors")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double cosSim(
            @SqlNullable @SqlType("array(double)") Block vec1,
            @SqlNullable @SqlType("array(double)") Block vec2)
    {
        if (!distIsDefined(vec1, vec2)) {
            return null;
        }

        double dotProd = 0.0;
        double vec1L2Norm = 0.0;
        double vec2L2Norm = 0.0;
        for (int position = 0; position < vec1.getPositionCount(); position++) {
            //todo can also use multi threads and let different threads be responsible for different elements
            // one thread for calculating x[1]*y[1], another x[2]*y[2]
            // let's keep it simple and only use single thread for now
            double xi = DOUBLE.getDouble(vec1, position);
            double yi = DOUBLE.getDouble(vec2, position);
            dotProd += xi*yi;
            vec1L2Norm += xi*xi;
            vec2L2Norm += yi*yi;
        }
        return dotProd / (Math.sqrt(vec1L2Norm) * Math.sqrt(vec2L2Norm));
    }

    private static boolean distIsDefined(Block vec1, Block vec2) {
        if (vec1!=null && vec2!=null && vec1.getPositionCount()==vec2.getPositionCount()) {
            return true;
        }
        return false;
    }
}
