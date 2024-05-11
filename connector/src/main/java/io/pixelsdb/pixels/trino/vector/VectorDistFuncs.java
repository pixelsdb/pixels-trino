package io.pixelsdb.pixels.trino.vector;

public class VectorDistFuncs {

    public enum DistFuncEnum {
        EUCLIDEAN_DISTANCE(VectorDistFuncs::eucDist),
        DOT_PRODUCT(VectorDistFuncs::dotProd),
        COSINE_SIMILARITY(VectorDistFuncs::cosSim);

        private final VectorDistFunc vectorDistFunc;

        DistFuncEnum(VectorDistFunc vectorDistFunc) {
            this.vectorDistFunc = vectorDistFunc;
        }

        public VectorDistFunc getDistFunc() {
            return vectorDistFunc;
        }

        public static DistFuncEnum getDistFuncEnumByOrdinal(int ordinal) {
            return values()[ordinal];
        }
    }

    public static Double eucDist(
            double[] vec1,
            double[] vec2)
    {
        if (!distIsDefined(vec1, vec2)) {
            return null;
        }

        double dist = 0.0;
        for (int i = 0; i < vec1.length; i++) {
            //todo can also use multi threads and let different threads be responsible for different elements
            // one thread for calculating (x[1]-y[1])^2, another (x[2]-y[2))^2
            // let's keep it simple and only use single thread for now
            double xi = vec1[i];
            double yi = vec2[i];
            dist += (xi-yi)*(xi-yi);
        }
        return dist;
    }

    public static Double dotProd(
            double[] vec1,
            double[] vec2)
    {
        if (!distIsDefined(vec1, vec2)) {
            return null;
        }

        double dist = 0.0;
        for (int i = 0; i < vec1.length; i++) {
            //todo can also use multi threads and let different threads be responsible for different elements
            // one thread for calculating x[1]*y[1], another x[2]*y[2]
            // let's keep it simple and only use single thread for now
            double xi = vec1[i];
            double yi = vec2[i];
            dist += xi*yi;
        }
        return dist;
    }


    public static Double cosSim(
            double[] vec1,
            double[] vec2)
    {
        if (!distIsDefined(vec1, vec2)) {
            return null;
        }

        double dotProd = 0.0;
        double vec1L2Norm = 0.0;
        double vec2L2Norm = 0.0;
        for (int position = 0; position < vec1.length; position++) {
            //todo can also use multi threads and let different threads be responsible for different elements
            // one thread for calculating x[1]*y[1], another x[2]*y[2]
            // let's keep it simple and only use single thread for now
            double xi = vec1[position];
            double yi = vec2[position];
            dotProd += xi*yi;
            vec1L2Norm += xi*xi;
            vec2L2Norm += yi*yi;
        }
        return dotProd / (Math.sqrt(vec1L2Norm) * Math.sqrt(vec2L2Norm));
    }

    private static boolean distIsDefined(double[] vec1, double[] vec2) {
        if (vec1!=null && vec2!=null && vec1.length==vec2.length) {
            return true;
        }
        return false;
    }
}
