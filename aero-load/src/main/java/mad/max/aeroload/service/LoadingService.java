package mad.max.aeroload.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.consumer.AerospikeAsyncOperateCaller;
import mad.max.aeroload.model.producer.LinesReaderConfigs;
import mad.max.aeroload.model.producer.LocalDirInputStreamProducer;
import mad.max.aeroload.model.producer.S3BucketInputStreamProducer;
import mad.max.aeroload.model.transformer.InputStreamParameterAdder;
import mad.max.aeroload.model.transformer.InputStreamToLinesAdapter;
import mad.max.aeroload.model.transformer.LinesToAerospikeObjectsAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LoadingService {

    @Value("${aerospike.host:localhost}")
    private String host;
    @Value("${aerospike.port:3000}")
    private int port;
    @Value("${aerospike.timeout:}")
    private int timeout;
    @Value("${aerospike.ttl-days:1}")
    private Integer ttl;
    @Value("${AWS.accesskey}")
    private String accessKey;
    @Value("${AWS.secretkey}")
    private String secretKey;
    @Value("${AWS.bucketName}")
    private String bucketName;
    @Value("${job.suggested.profile:DEFAULT}")
    private LinesReaderConfigs linesReaderConfigs;
    private String strategy;

    public void load(LoadingProfile loadingProfile) {

        try (AerospikeClient client = aerospikeClient(loadingProfile)) {
            AerospikeAsyncOperateCaller aerospikeLoader = new AerospikeAsyncOperateCaller(client, loadingProfile);
            LinesToAerospikeObjectsAdapter linesToAerospikeObjectsAdapter = new LinesToAerospikeObjectsAdapter(aerospikeLoader);
            InputStreamToLinesAdapter s3InputStreamToLinesAdapter = new InputStreamToLinesAdapter(linesToAerospikeObjectsAdapter);
            InputStreamParameterAdder inputStreamParameterAdder = new InputStreamParameterAdder(linesReaderConfigs, s3InputStreamToLinesAdapter);
            var producer = switch (strategy) {
                case "S3" -> new S3BucketInputStreamProducer(s3Client(accessKey, secretKey), bucketName, inputStreamParameterAdder);
                case "local" -> new LocalDirInputStreamProducer(inputStreamParameterAdder, bucketName);
            };
            aerospikeLoader.spinOff();//The aerospikeLoader goes off in another thread...
            producer.run();
            aerospikeLoader.waitToFinish();
            System.out.println(aerospikeLoader.stats());
        }
    }


    public AerospikeClient aerospikeClient(LoadingProfile profile) {
        EventPolicy eventPolicy = new EventPolicy();

        //limit maxCommandsInProcess on each event loop.
        // This is the thread-safe way to limit inflight commands (and thus connections).
        eventPolicy.maxCommandsInProcess = profile.getMaxParallelCommands();

        //The async command queue is used for commands when maxCommandsInProcess limit is reached.
        // If the rate of incoming commands consistently exceeds the rate at which commands are processed,
        // then your application can potentially run out of memory.
        // To avoid this case, you may need to limit your queue size.
        eventPolicy.maxCommandsInQueue = profile.getMaxParallelCommands();

        EventLoops eventLoops = new NioEventLoops(eventPolicy, Runtime.getRuntime().availableProcessors());

        ClientPolicy policy = new ClientPolicy();
        policy.eventLoops = eventLoops;
        policy.maxConnsPerNode = eventPolicy.maxCommandsInProcess * eventLoops.getSize();

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.setTimeout(timeout);
        writePolicy.sendKey = true;
        writePolicy.expiration = ttl * 86400;
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        policy.writePolicyDefault = writePolicy;

        Host[] hosts = Host.parseHosts(host, port);

        return new AerospikeClient(policy, hosts);
    }

    public static AmazonS3 s3Client(String accessKey, String secretKey) {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withRegion(Regions.US_EAST_2)
                .build();
    }
}
