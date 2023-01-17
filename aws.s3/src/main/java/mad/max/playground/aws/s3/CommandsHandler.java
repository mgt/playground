package mad.max.playground.aws.s3;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static mad.max.playground.aws.s3.Utils.*;

@ShellComponent
public class CommandsHandler {

    private final S3Client s3Client;

    public CommandsHandler(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @ShellMethod("Upload file (sourcePath) set (key) in bucket (bucket).")
    public void uploadFile(String sourcePath, String key, String bucket) {
        try {
            System.out.printf("Uploading object %s with key %s to bucket %s%n", sourcePath, key, bucket);
            System.out.println("...");
            s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                    .build(), Path.of(sourcePath));
            System.out.println("Upload is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.err.printf("%n");
        }
    }

    @ShellMethod("Create s3 (bucket).")
    public void createBucket(String bucket) {
        try {
            s3Client.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucket)
                    .build());
            System.out.println("Creating bucket: " + bucket);
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            System.out.printf("Bucket %s is ready. %n", bucket);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.err.printf("%n");
        }
    }

    @ShellMethod("Delete s3 object by (key) and (bucket).")
    public void deleteObject(String bucket, String key) {
        System.out.println("Cleaning up...");
        try {
            System.out.println("Deleting object: " + key);
            DeleteObjectRequest deleteObjectRequest =
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build();
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println(key + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.err.printf("%n");
        }
    }

    @ShellMethod("Delete s3 (bucket).")
    public void deleteBucket(String bucket) {
        try {
            System.out.println("Deleting bucket: " + bucket);
            DeleteBucketRequest deleteBucketRequest =
                    DeleteBucketRequest.builder().bucket(bucket).build();
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println(bucket + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.err.printf("%n");
        }
    }

    @ShellMethod("List all s3 buckets.")
    public void listBuckets() {
        try {
            List<Bucket> buckets = s3Client.listBuckets().buckets();
            for (Bucket bucket : buckets) {
                System.out.println(bucket.name());
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.err.printf("%n");
        }
    }

    @ShellMethod("list all s3 objects in a (bucket).")
    public void listBucketObjects( String bucket ) {

        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucket)
                    .build();

            ListObjectsResponse res = s3Client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            List<List<String>> rows = new ArrayList<>();

            if(objects.size() ==0)
                System.out.printf("No objects in bucket %s%n", bucket);

            List<String> headers = Arrays.asList("Key", "Size", "Owner", "ETag", "Modification Time");
            rows.add(headers);
            for (S3Object myValue : objects) {
                rows.add(List.of(myValue.key()+"", toKb(myValue.size()) + "",myValue.owner() + "", myValue.eTag(),
                        formatInstant(myValue.lastModified())) );
            }

            System.out.println(formatAsTable(rows));
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.err.printf("%n");
        }
    }

    @ShellMethod("Quit program.")
    public void exit() {
        try {
            s3Client.close();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.err.printf("%n");
        }
        System.exit(0);
    }
}
