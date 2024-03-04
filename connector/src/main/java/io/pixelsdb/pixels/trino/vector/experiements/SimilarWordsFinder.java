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

    public static void main(String[] args) {
        /* get words for resulting vectors */
        Pair<String, Double>[] wordToDist = getWordsAndDistancesForVecs("/Users/sha/Desktop/EPFL/master-thesis/vecdb-experiment-results/exact-nns-Switzerland5.txt", VectorDistFuncs::eucDist);
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

        /* get vectors for word */
        String word = "Switzerland";
        System.out.println(Arrays.toString(getEmbeddingForWord(word)));
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
                        double dist = vectorDistFunc.getDist(embeddings[0], vec);
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
