package mad.max.aeroload.job;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.cdt.ListOperation;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.JobConfig;
import mad.max.aeroload.model.AerospikeLoader;
import mad.max.aeroload.model.FileLineKeyOpProducer;
import mad.max.aeroload.model.FileReaderProducer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.StopWatch;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static mad.max.aeroload.model.FileLineKeyOpProducer.BIN_SEGMENT_NAME;
import static mad.max.aeroload.model.FileLineKeyOpProducer.POLICY;
import static mad.max.aeroload.model.FileLineKeyOpProducer.getKey;

@Slf4j
@ShellComponent
public class Job {
    private final AerospikeClient client;
    private final Throttles throttles;
    private final JobConfig jobConfig;

    Job(AerospikeClient client, Throttles throttles, JobConfig jobConfig) {
        this.client = client;
        this.throttles = throttles;
        this.jobConfig = jobConfig;
    }

    @ShellMethod("Run job")
    public void run(long limit) {
        AerospikeLoader loader = new AerospikeLoader(client, throttles, jobConfig.getMaxThroughput());
        loader.spinTask();
        FileLineKeyOpProducer fileLineKeyOpProducer = new FileLineKeyOpProducer(loader);
        StopWatch timeMeasure = new StopWatch();
        timeMeasure.start("Run job");
        FileReaderProducer fileReaderProducer =
                new FileReaderProducer(jobConfig.getDelimiter(), jobConfig.getSegmentDelimiter(), jobConfig.getSegmentIndexInFile(),  new File(jobConfig.getFilePath()), jobConfig.isHasHeader(), fileLineKeyOpProducer);
        fileReaderProducer.run( limit <= 0?Long.MAX_VALUE:limit );
        timeMeasure.stop();
        System.out.println(timeMeasure.prettyPrint());
        System.out.println(loader.stats());
        loader.waitToFinish();
    }

    @ShellMethod("check key")
    public void check(String k) {
        Key key = getKey(k);
        Record record = client.get(client.readPolicyDefault, key);
        System.out.printf("key:%s%n", k);
        System.out.printf("size:%d%n", record.getList(BIN_SEGMENT_NAME).size());
        System.out.printf("ttl:%d%n", record.getTimeToLive());
        System.out.println(record);
    }

    @ShellMethod("insert forced")
    public void insertForce(String k, int count, String putMode) {
        Key key = getKey(k);
        Stream<String> str = IntStream.range(0, count)
                .mapToObj(i -> "100" + String.format("%07d", i));
        if ("string".equals(putMode)) {
            client.put(client.writePolicyDefault, key, new Bin(BIN_SEGMENT_NAME, str.collect(Collectors.joining(","))));
        } else {
            Operation[] operations =
                    str
                            .map(com.aerospike.client.Value::get)
                            .map(v -> ListOperation.append(POLICY, BIN_SEGMENT_NAME, v))
                            .toArray(Operation[]::new);
            log.debug("inserting {} values  into {}", count, BIN_SEGMENT_NAME);
            client.operate(client.writePolicyDefault, key, operations);
        }
    }

}
