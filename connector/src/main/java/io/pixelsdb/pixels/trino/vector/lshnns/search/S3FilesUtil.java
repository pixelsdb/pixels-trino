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

    public static List<String> listBucketObjects(String bucketName) {
        Region region = Region.US_EAST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        List<String> files = new ArrayList<>();
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            for (S3Object s3obj : res.contents()) {
                files.add(s3obj.key());
            }
//            for (S3Object myValue : objects) {
//                System.out.print("\n The name of the key is " + myValue.key());
//                System.out.print("\n The object is " + calKb(myValue.size()) + " KBs");
//                System.out.print("\n The owner is " + myValue.owner());
//            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        s3.close();
        return files;
    }

    public static String fileNameToHashKeyStr(String fileName) {
        int end = fileName.lastIndexOf('_');
        return fileName.substring(0, end);
    }
}
