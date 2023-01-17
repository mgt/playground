package mad.max.playground.aws.s3;

import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

@org.springframework.context.annotation.Configuration
public class Configuration {

    /**
     * @return an instance of S3Client
     */
    @Bean
    public S3Client s3Client() {
        return S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .build();
    }
}
