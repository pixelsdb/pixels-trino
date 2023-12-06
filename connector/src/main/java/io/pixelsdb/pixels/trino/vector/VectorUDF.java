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
        //todo deploy and set up debbug environment

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
}
