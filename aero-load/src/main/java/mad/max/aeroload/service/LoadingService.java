package mad.max.aeroload.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.AerospikeLoader;
import mad.max.aeroload.model.FileLineKeyOpProducer;
import mad.max.aeroload.model.FileReaderProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import java.io.File;

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
    @Value("${file.path}")
    private String filePath;


    public void load(LoadingProfile loadingProfile) {
        Throttles throttles = new Throttles(Runtime.getRuntime().availableProcessors(), loadingProfile.getMaxParallelCommands());

        try (AerospikeClient client = aerospikeClient(loadingProfile)) {
            AerospikeLoader loader = new AerospikeLoader(client, throttles, loadingProfile.getMaxThroughput(), loadingProfile.getMaxQueuedElements());
            loader.spinOff();
            FileLineKeyOpProducer fileLineKeyOpProducer = new FileLineKeyOpProducer(loader);
            StopWatch timeMeasure = new StopWatch();
            timeMeasure.start("Run job");
            FileReaderProducer fileReaderProducer = new FileReaderProducer(fileLineKeyOpProducer);

            FileReaderProducer.ReadingConfig readingConfig = FileReaderProducer.Configs.DEFAULT.getReadingConfig();
            FileReaderProducer.Parameters parameters =
                    new FileReaderProducer.Parameters(new File(filePath), 0, loadingProfile.getMaxLinesPerFile());
            fileReaderProducer.run(parameters, readingConfig);
            loader.waitToFinish();
            timeMeasure.stop();
            log.info(timeMeasure.prettyPrint());
            log.info(loader.stats());
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
