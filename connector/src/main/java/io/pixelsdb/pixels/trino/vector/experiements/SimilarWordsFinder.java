package io.pixelsdb.pixels.trino.vector.experiements;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import io.pixelsdb.pixels.trino.vector.VectorDistFunc;
import io.pixelsdb.pixels.trino.vector.VectorDistFuncs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class SimilarWordsFinder {

    public static final String vocabularyFile = "/Users/sha/Desktop/EPFL/master-thesis/wiki-news-300d-1m.vec";
    static ObjectMapper objectMapper = new ObjectMapper();
    static final double[] QUEEN_VEC = {0.2158,0.1095,-0.0499,0.0528,-0.0691,0.1357,-0.2257,-0.0401,-0.127,0.0628,-0.0031,-0.0278,0.0962,-0.0509,0.1659,-0.1456,0.0043,-0.0858,0.0675,-0.1441,-0.1971,0.0238,-0.1019,0.0023,-0.1479,-0.0579,-0.0348,0.1964,0.131,0.0026,0.1745,0.1163,-0.0067,0.0843,0.0498,-0.0916,-0.0876,0.0906,0.0348,-6.0E-4,0.1479,-0.037,-0.049,-0.1296,0.0063,0.1218,-0.0154,0.0408,-0.0499,0.0074,-0.0628,-0.1445,-0.6658,0.0405,0.1376,0.0919,0.0064,0.1542,0.0345,-0.142,-0.0065,-0.0346,-0.1175,0.017,0.0975,0.0143,0.1287,-0.1075,-0.0065,0.0312,-0.0693,-0.116,-0.0655,-0.0168,-0.0913,-0.0935,-0.0625,-0.131,-0.1675,0.1654,-0.0291,0.1045,0.1013,-0.2298,-0.0114,-0.0483,-0.0833,-0.0197,0.2074,0.0536,-0.078,0.1643,-0.1019,-0.0931,-0.1721,0.1074,-0.1172,-0.1924,0.0593,0.2065,-0.1203,-0.0467,0.1084,0.0567,-0.0726,0.1413,0.025,0.1973,-0.0504,-0.1155,0.1588,0.1433,-0.0268,0.0863,-0.0997,-0.0466,-0.3265,-0.0673,-0.2185,-0.3463,-0.0872,-0.2026,0.0909,-0.0537,0.0585,0.1235,0.0444,-0.048,0.0677,-0.0741,0.0913,0.0058,-0.055,-0.0142,0.0055,-0.0351,0.1426,-0.0439,-0.1415,-0.0103,-0.0261,-0.0491,0.1112,0.2555,-0.0204,0.0381,0.1636,0.04,-0.0657,0.0045,-0.0749,-0.1928,-0.0147,-0.1681,0.0318,0.177,0.1891,0.1022,-0.1247,0.1407,0.0687,-0.3527,-0.1691,0.1944,0.0327,0.083,0.0782,0.0804,-0.0624,-0.0398,-0.0075,-0.082,-0.0755,0.0504,0.1733,-0.0063,0.2813,0.0388,-0.0612,0.0538,-0.1038,0.0091,-0.1261,0.0584,-0.0394,-0.0677,0.0403,-0.0526,-0.1908,0.0883,-0.0173,-0.0609,-0.0514,0.0405,0.0013,0.0893,-0.0247,-0.0738,0.1093,0.2395,0.0624,-0.0682,-0.2574,0.0557,0.0258,0.1199,-0.0422,-0.012,-0.1217,-0.0582,0.0242,0.0149,0.1039,0.0624,-0.1623,-0.0538,0.0108,-0.1172,0.0243,-0.0471,-0.0398,-0.1916,-0.1612,-0.0712,0.063,-0.1812,0.01,-0.072,0.0633,-0.0304,0.0055,0.0877,0.3299,-0.1671,-0.0814,-0.1093,-0.0552,0.1108,-0.2203,-0.1218,-0.0576,0.1252,-0.0136,0.1349,0.1234,0.0827,-0.1832,0.155,-0.159,0.3917,0.0217,0.012,0.0074,-0.3095,0.076,0.0258,-0.0027,-0.1155,0.2152,-0.0023,-0.0116,0.0667,-0.0752,0.0392,-0.345,-0.0493,0.0098,-0.2498,-0.1739,-0.0746,-0.1962,0.2262,0.0944,0.0789,0.0607,0.3018,-0.0569,0.0931,0.0977,0.2114,0.0645,0.0111,-0.1061,-0.0148,0.1037,0.0244,4.0E-4,-0.1368,0.1,-0.0398,0.0114,-0.1902,0.1368,-0.1466,0.1036,0.0302,-0.0502,0.0857,0.102,0.0424};

    public static void main(String[] args) {

        /* get words for resulting vectors */
        Pair<String, Double>[] wordToDist = getWordsAndDistancesForVecs("/Users/sha/Desktop/EPFL/master-thesis/vecdb-experiment-results/lsh-queen5-8192buckets.txt", VectorDistFuncs::eucDist);
        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder.append("[");
//        for (int i=0; i < wordToDist.size(); i++) {
//            stringBuilder.append("\"");
//            stringBuilder.append(words[i]);
//            stringBuilder.append("\"");
//            if (i < words.length-1) {
//                stringBuilder.append(", ");
//            }
//        }
//        stringBuilder.append("]");
//        System.out.println(stringBuilder);
        System.out.println(Arrays.toString(wordToDist));
        double distSum = 0.0;
        for (int i=0; i < wordToDist.length; i++) {
            distSum += wordToDist[i].getValue();
        }
        System.out.println("avg dist: " + distSum/ wordToDist.length);

        /* get vectors for word */
//        String word = "Switzerland";
//        System.out.println(Arrays.toString(getEmbeddingForWord(word)));
    }

    private static Pair<String, Double>[] getWordsAndDistancesForVecs(String vecFile, VectorDistFunc vectorDistFunc) {
        List<String> result = new ArrayList<>();
        
        String line;
        //List<double[]> vecs = new ArrayList<>();
        double[][] vecs=null;
        try (BufferedReader br = new BufferedReader(new FileReader(vecFile))) {
            line = br.readLine();
            line = line.substring(1,line.length()-1);
            vecs = objectMapper.readValue(line, double[][].class);
            //            while ((line = br.readLine()) != null ) {
//                vecs = objectMapper.readValue(line, double[][].class);
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return getWordsAndDistsForEmbeddings(vecs, vectorDistFunc);
    }

    private static double[] getEmbeddingForWord(String word) {
        //todo
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(vocabularyFile))) {
            int dimension = Integer.parseInt(br.readLine().split("\\s+")[1]);
            while ((line = br.readLine()) != null ) {
                String[] wordAndEmbedding = line.split("\\s+");
                String currWord = wordAndEmbedding[0];
                if (currWord.equals(word)) {
                    double[] vec = new double[dimension];
                    for (int i=0; i < dimension; i++) {
                        vec[i] = Double.parseDouble(wordAndEmbedding[i+1]);
                    }
                    return vec;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Pair<String, Double>[] getWordsAndDistsForEmbeddings(double[][] embeddings, VectorDistFunc vectorDistFunc) {
        Pair<String, Double>[] wordToDist = new Pair[embeddings.length];
        String line;
        int dimension = embeddings[0].length;
        try (BufferedReader br = new BufferedReader(new FileReader(vocabularyFile))) {
            br.readLine(); // read the first line
            while ((line = br.readLine()) != null ) {
                String[] wordAndEmbedding = line.split("\\s+");

                // read the word and vec in this line
                String word = wordAndEmbedding[0];
                double[] vec = new double[dimension];
                for (int i=0; i < dimension; i++) {
                    vec[i] = Double.parseDouble(wordAndEmbedding[i+1]);
                }

                // check whether current vec equal to the input embeddings
                for (int i=0; i < embeddings.length; i++) {
                    if (equalVecs(embeddings[i], vec)) {
                        double dist = vectorDistFunc.getDist(QUEEN_VEC, vec);
                        wordToDist[i] = Pair.of(word, dist);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wordToDist;
    }

    private static boolean equalVecs(double[] vec1, double[] vec2) {
        assert(vec1.length == vec2.length);
        for (int i=0; i<vec1.length; i++) {
            if (Math.abs(vec1[i] - vec2[i]) > 1e-5) {
                return false;
            }
        }
        return true;
    }
}
