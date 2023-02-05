package mad.max.aeroload.model.producer;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.Md5Utils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.producer.base.FileSystem;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.InputStream;
import java.util.List;

@Slf4j
public class S3FileSystem implements FileSystem {

    private final AmazonS3 s3client;
    private final String bucketName;

    public S3FileSystem(AmazonS3 s3Client, String bucketName) {
        this.s3client = s3Client;
        this.bucketName = bucketName;
    }

    public Iterable<String> ls() {
        ObjectListing objectListing = s3client.listObjects(bucketName);
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        return objectSummaries.stream().map(S3ObjectSummary::getKey).toList();
    }

    public Pair<InputStreamMeta, InputStream> get(String key) {
        S3Object s3object = s3client.getObject(bucketName, key);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        s3object.getObjectMetadata().getContentLength();
        InputStreamMeta inputStreamMeta = new InputStreamMeta(s3object.getKey(),
                s3object.getObjectMetadata().getContentLength(),
                Md5Utils.md5AsBase64(DigestUtils.md5(s3object.getObjectMetadata().getContentMD5())),
                s3object.getObjectMetadata().getLastModified());
        return new Pair<>(inputStreamMeta, inputStream);
    }

    @Override
    public boolean fileExist(String fileName) {
        return s3client.doesObjectExist(bucketName, fileName);
    }

    @SneakyThrows
    public void createFile(String fileName, String content) {
        s3client.putObject(bucketName, fileName, content);
    }
}
