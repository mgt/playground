package mad.max.aeroload.model.producer;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Triad;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.base.AsyncProducer;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class S3BucketInputStreamProducer extends AsyncProducer<Triad<String, String, InputStream>> {

    private final AmazonS3 s3client;

    public S3BucketInputStreamProducer(AmazonS3 s3Client, AsyncConsumer<Triad<String, String, InputStream>> consumer) {
        super(consumer);
        this.s3client=s3Client;
    }

    public void run(String bucketName) {
        ObjectListing objectListing = s3client.listObjects(bucketName);
        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
            S3Object s3object = s3client.getObject(bucketName, os.getKey());
            S3ObjectInputStream inputStream = s3object.getObjectContent();
                this.push(new Triad<>(s3object.getKey(), os.getETag(), inputStream), new AsyncConsumer.Observer() {
                    @Override
                    public void onSuccess() {

                        closeAwsInput();
                    }

                    @Override
                    public void onFail(String error) {

                        closeAwsInput();
                    }

                    private void closeAwsInput() {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            // We are guesses in this party, shouldn't throw exception (?)
                            // throw new RuntimeException(e);
                        }
                    }
                });
            }
        }
}
