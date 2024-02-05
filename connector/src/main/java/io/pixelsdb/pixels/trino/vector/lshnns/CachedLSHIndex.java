package io.pixelsdb.pixels.trino.vector.lshnns;

import io.pixelsdb.pixels.trino.PixelsColumnHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CachedLSHIndex {
    // the column queried by current query
    static PixelsColumnHandle currColumn;
    static String currTable;

    // map each column to the directory on s3 which stores the indexed files
    // in that dir maybe we can use file names to represent the hash value. if within 64 bits, then
    // only one long value.

    // we can also store a mapping between hashkey to a list of files that have that hash, and we might also add files from hashes that are close to the hashkey. evntually we pass a scanset to the pixelsreader

    // we can first create a non-indexed table and then create an indexed table from the original table by doing create table (s3 path=...) and then call:
    // select build_lsh(arr_col, numBits, indexed_table_path) from original table

    // or maybe just use udf build_lsh(s3_file_dir)
    // the bottom line is if we want to use agg func to auto distributedly read all files, we would
    // need an indexed table with a dir storing all the bucketed files. And then maybe we can specify
    // hamming distance to specify which buckets to include and do exact_nns_agg  with other files pruned

    // alternatively can also just write bucketed files to a s3 dir and then simply use udf to do:
    // select lsh_nns(schema.table.col, inputVec) and then use col and hash(inputVec) to find what files
    // to read and read those files in a single machine.


    static HashMap<PixelsColumnHandle, String> colToBucketsDir = new HashMap<>();
    static HashMap<PixelsColumnHandle, LSHFunc> colToLSHFunc = new HashMap<>();

    public static PixelsColumnHandle getCurrColumn() {
        return currColumn;
    }

    public static String getCurrTable() {
        return currTable;
    }

    public static void setCurrColumn(PixelsColumnHandle currColumn) {
        CachedLSHIndex.currColumn = currColumn;
    }

    public static void setCurrTable(String currTable) {
        CachedLSHIndex.currTable = currTable;
    }

    public static void updateColToBucketedData(String tableS3Path) {
        // when executing an LSH build query, the currColumn will be updated earlier when trino calls pixels to create page source, which is at the bottom of the pixels-trino call stack
        colToBucketsDir.put(currColumn, tableS3Path);
    }

    public static void updateColToBuckets(String tableS3Path, LSHFunc lshFunc) {
        colToBucketsDir.put(currColumn, tableS3Path);
        colToLSHFunc.put(currColumn, lshFunc);
    }

    public static void updateColToLSHFunc(LSHFunc lshFunc) {
        // when executing an LSH build query, the currColumn will be updated earlier when trino calls pixels to create page source, which is at the bottom of the pixels-trino call stack
        colToLSHFunc.put(currColumn, lshFunc);
    }

    public static String getBucketsDir() {
        return colToBucketsDir.get(currColumn);
    }

    public static LSHFunc getLSHFunc() {
        return colToLSHFunc.get(currColumn);
    }
}
