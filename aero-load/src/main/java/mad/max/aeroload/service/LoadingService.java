package mad.max.aeroload.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.consumer.AerospikeAsyncOperateCaller;
import mad.max.aeroload.model.consumer.AerospikeParameters;
import mad.max.aeroload.model.producer.InputStreamProducer;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import mad.max.aeroload.model.producer.base.filesystem.FileSystem;
import mad.max.aeroload.model.transformer.InputStreamToLines;
import mad.max.aeroload.model.transformer.LinesReadingParameters;
import mad.max.aeroload.model.transformer.LinesToAerospikeObjects;
import mad.max.aeroload.model.transformer.LinesToAerospikeParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static mad.max.aeroload.model.producer.base.LinesReaderConfigs.SEGMENTS;
import static mad.max.aeroload.model.producer.base.LinesReaderConfigs.SEGMENT_BIN_NAME;

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

    @Autowired
    private FileSystem fs;

    public static final String SET_NAME = "audience_targeting_segments";
    public static final String NAMESPACE = "tempcache";

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public void load(LoadingProfile loadingProfile) {

        AerospikeParameters aerospikeParameters = new AerospikeParameters(loadingProfile.getMaxErrorThreshold(),
                loadingProfile.getMaxThroughput(),
                loadingProfile.getMaxParallelCommands(),
                loadingProfile.getMaxQueuedElements());

        LinesToAerospikeParameters linesToAerospikeParameters = new LinesToAerospikeParameters(NAMESPACE, SET_NAME, f -> SEGMENT_BIN_NAME,
                SEGMENTS.getReadingConfig().delimiter(),
                SEGMENTS.getReadingConfig().segmentColumnIndexInFile(),
                SEGMENTS.getReadingConfig().segmentDelimiter());

        LinesReadingParameters linesReadingParameters = new LinesReadingParameters(0, loadingProfile.getMaxLinesPerFile(),
                loadingProfile.getMaxErrorThreshold(),
                SEGMENTS.getReadingConfig().hasHeader(),
                SEGMENTS.getReadingConfig().isCompressed());


        try (AerospikeClient client = aerospikeClient(loadingProfile)) {
            // ↓  ↓  ↓ The code is better understood reading it bottom up.
            //========================================================================================================
            //This is the synk, all things that got here through the chain will end up in aerospike
            AerospikeAsyncOperateCaller aerospikeLoader = new AerospikeAsyncOperateCaller(client, aerospikeParameters);
            //This takes the line from the previous step and creates one key/operation(1+) and pushes ↑ to the next chain
            LinesToAerospikeObjects linesToAerospikeObjects = new LinesToAerospikeObjects(aerospikeLoader, linesToAerospikeParameters);
            //This reads the inputStream and creates one element per line then pushes ↑ to the next in the chain
            InputStreamToLines inputStreamToLines = new InputStreamToLines(linesToAerospikeObjects, linesReadingParameters);
            //Create a producer, to push elements to the next in the chain ↑
            //fs:Filesystem, encapsulate files operations
            InputStreamProducer producer = new InputStreamProducer(inputStreamToLines, fs);
            //↑  ↑  ↑  From this point  ↑  ↑  ↑
            //========================================================================================================

            aerospikeLoader.spinOff();//The aerospikeLoader goes off in another thread...

            //Configuring the producer
            producer.addFilter(im -> im.contentLength() > 0); //to ignore folders
            producer.addFilter(im -> im.fileName().contains(SEGMENTS.getReadingConfig().folderLocation())); //to only explore subfolder we are interested
            producer.addFilter(im->SEGMENTS.getReadingConfig().namePatter().matcher(im.fileName()).matches() ); //to only accept files matching name
            // discarding the future. No need to use it here.
            ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(() -> log.info(aerospikeLoader.stats()), 10L, 10L, TimeUnit.SECONDS);
            producer.run(); //These producers are Runnable just for convenience

            aerospikeLoader.waitToFinish();
            linesToAerospikeObjects.close();
            scheduledFuture.cancel(true);
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


}
