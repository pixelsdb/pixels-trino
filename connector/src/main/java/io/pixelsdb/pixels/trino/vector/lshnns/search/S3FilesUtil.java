package io.pixelsdb.pixels.trino.vector.lshnns.search;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;


public class S3FilesUtil {

    public static List<String> listBucketObjects(String s3Path) {
        Region region = Region.US_EAST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        List<String> files = new ArrayList<>();
        String[] bucketAndPrefix = s3PathToBucketNameAndPrefix(s3Path);
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketAndPrefix[0])
                    .prefix(bucketAndPrefix[1])
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            for (S3Object s3obj : res.contents()) {
                files.add(s3FilePathToFileName(s3obj.key()));
            }
//            for (S3Object myValue : objects) {
//                System.out.print("\n The name of the key is " + myValue.key());
//                System.out.print("\n The object is " + calKb(myValue.size()) + " KBs");
//                System.out.print("\n The owner is " + myValue.owner());
//            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        s3.close();
        return files;
    }

    /**
     *
     * @param s3Path
     * @param hashKey the hashkey in string form. e.g. if 512 buckets, then "511_" should be a hash key
     * @return
     */
    public static List<String> listFilesWithHashKey(String s3Path, String hashKey) {
        Region region = Region.US_EAST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        List<String> files = new ArrayList<>();
        String[] bucketAndPrefix = s3PathToBucketNameAndPrefix(s3Path);
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketAndPrefix[0])
                    .prefix(bucketAndPrefix[1] + hashKey)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            for (S3Object s3obj : res.contents()) {
                files.add(s3FilePathToFileName(s3obj.key()));
            }
//            for (S3Object myValue : objects) {
//                System.out.print("\n The name of the key is " + myValue.key());
//                System.out.print("\n The object is " + calKb(myValue.size()) + " KBs");
//                System.out.print("\n The owner is " + myValue.owner());
//            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        s3.close();
        return files;
    }

    public static String fileNameToHashKeyStr(String fileName) {
        int end = fileName.lastIndexOf('_');
        return fileName.substring(0, end+1);
    }

    public static String[] s3PathToBucketNameAndPrefix(String s3Path) {
        // Remove "s3://" prefix
        String pathWithoutPrefix = s3Path.substring(5);

        // Split into bucket name and prefix
        String[] parts = pathWithoutPrefix.split("/", 2);
        String bucketName = parts[0];
        String prefix = parts.length > 1 ? parts[1] : "";
        return parts;
    }

    public static String s3FilePathToFileName(String s3FilePath) {
        // Find the last occurrence of "/"
        int lastIndex = s3FilePath.lastIndexOf('/');

        // Extract the file name
        return s3FilePath.substring(lastIndex + 1);
    }
}
