package mad.max.aeroload.job;

import com.aerospike.client.AerospikeClient;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import mad.max.aeroload.utils.AeroUtils;
import mad.max.aeroload.model.producer.base.filesystem.LocalFileSystem;
import mad.max.aeroload.model.producer.base.filesystem.S3FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {
    @Value("${aerospike.host:localhost}")
    private String host;
    @Value("${aerospike.port:3000}")
    private int port;
    @Value("${aerospike.timeout:}")
    private int timeout;
    @Value("${aerospike.ttl-days:1}")
    private Integer ttl;


    @Bean(destroyMethod = "close")
    public AerospikeClient aerospikeClient() {
        return AeroUtils.aerospikeClient( host, port,timeout,ttl, 100);
    }

    @Bean
    @ConditionalOnProperty(value = "job.fileSystem", havingValue = "local", matchIfMissing = true)
    LocalFileSystem localFileSystem(@Value("${job.fileSystem.local.folder}") String folderName) {
        //Filesystem, encapsulate files operations
        return new LocalFileSystem(folderName);
    }

    @Bean
    @ConditionalOnProperty(value = "job.fileSystem", havingValue = "s3")
    S3FileSystem s3FileSystem(@Value("${job.fileSystem.s3.bucketName}") String bucketName,
                            @Value("${AWS.secretkey}") String secretKey,
                            @Value("${AWS.accesskey}") String accessKey) {
        return new S3FileSystem(s3Client(accessKey, secretKey), bucketName);
    }

    public static AmazonS3 s3Client(String accessKey, String secretKey) {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withRegion(Regions.US_EAST_2)
                .build();
    }

}
