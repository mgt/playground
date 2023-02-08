package mad.max.aeroload.job;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.ListOperation;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.service.LoadingProfile;
import mad.max.aeroload.service.LoadingService;
import mad.max.aeroload.utils.UUIDUtils;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.StopWatch;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static mad.max.aeroload.model.producer.base.LinesReaderConfigs.SEGMENT_BIN_NAME;
import static mad.max.aeroload.model.transformer.LinesToAerospikeObjects.POLICY;
import static mad.max.aeroload.service.LoadingService.NAMESPACE;
import static mad.max.aeroload.service.LoadingService.SET_NAME;

@Slf4j
@ShellComponent
public class ShellSupport {
    private final AerospikeClient client;
    private final LoadingService loadingService;

    ShellSupport(AerospikeClient client, LoadingService loadingService) {
        this.client = client;
        this.loadingService = loadingService;
    }

    @ShellMethod("Run job")
    public void run(@ShellOption(defaultValue = "DEFAULT") LoadingProfile.PredefinedProfiles profile, @ShellOption(defaultValue = "0")  long limit) {
        StopWatch timeMeasure = new StopWatch();
        timeMeasure.start();
        LoadingProfile p = profile.getProfile();
        loadingService.load(new LoadingProfile(p.getMaxThroughput(), limit > 0 ? limit : p.getMaxLinesPerFile(), p.getMaxParallelCommands(), p.getMaxQueuedElements(),p.getMaxErrorThreshold()));
        timeMeasure.stop();
        System.out.printf("#run time=%dms%n", timeMeasure.getLastTaskTimeMillis());
    }

    @ShellMethod("check key")
    public void check(String k) {
        Key key = getKey(k);
        Record record = client.get(client.readPolicyDefault, key);
        System.out.printf("key:%s%n", k);
        System.out.printf("size:%d%n", record.getList(SEGMENT_BIN_NAME).size());
        System.out.printf("ttl:%d%n", record.getTimeToLive());
        System.out.println(record);
    }

    @ShellMethod("operation to try limit on one key")
    public void tryLimit(String k, @ShellOption(value = "c", defaultValue = "10000") int count, @ShellOption(value = "m", defaultValue = "v") String putMode) {
        Key key = getKey(k);
        Stream<String> str = IntStream.range(0, count)
                .mapToObj(i -> "100" + String.format("%07d", i));
        if ("string".equals(putMode)) {
            client.put(client.writePolicyDefault, key, new Bin(SEGMENT_BIN_NAME, str.collect(Collectors.joining(","))));
        } else {
            Operation[] operations =
                    str
                            .map(com.aerospike.client.Value::get)
                            .map(v -> ListOperation.append(POLICY, SEGMENT_BIN_NAME, v))
                            .toArray(Operation[]::new);
            client.operate(client.writePolicyDefault, key, operations);
        }
    }

    @ShellMethod("clean aerospike namespace and set")
    public void clean() {
        client.truncate(null, NAMESPACE, SET_NAME, null);
    }

    @ShellMethod("operation to insert an amount of records")
    public void insertBulk(@ShellOption(value = "c", defaultValue = "1000000") int count, @ShellOption(value = "m", defaultValue = "v") String putMode) {
        StopWatch timeMeasure = new StopWatch();
        timeMeasure.start();
        List<String> segments = Arrays.asList("1000000000,1000000001,1000000002,1000000003,1000000004,1000000005,1000000006,1000000007,1000000008,1000000009".split(","));

        List<Key> keys = IntStream.range(0, count)
                .mapToObj(i -> ThreadLocalRandom.current().nextLong())
                .map(i -> getKey(UUIDUtils.nameUUIDFromNamespaceAndString(UUIDUtils.NAMESPACE, i + "")))
                .toList();


        for (Key key : keys) {

            if ("string".equals(putMode)) {
                client.put(client.writePolicyDefault, key, new Bin(SEGMENT_BIN_NAME,
                        String.join(",", segments.subList(0, ThreadLocalRandom.current().nextInt(1, segments.size())))));
            } else {
                Operation[] operations =
                        segments.subList(0, ThreadLocalRandom.current().nextInt(1, segments.size()))
                                .stream()
                                .map(com.aerospike.client.Value::get)
                                .map(v -> ListOperation.append(POLICY, SEGMENT_BIN_NAME, v))
                                .toArray(Operation[]::new);
                client.operate(client.writePolicyDefault, key, operations);
            }

        }

        timeMeasure.stop();
        System.out.println("insert-force running time =" + timeMeasure.getLastTaskTimeMillis());
    }


    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(8);

    @SneakyThrows
    @ShellMethod("operation to insert an amount of records")
    public void testReads(@ShellOption(value = "c", defaultValue = "1000000") int count,
                          @ShellOption(value = "m", defaultValue = "v") String putMode,
                          @ShellOption(value = "%", defaultValue = "20") int percent,
                          @ShellOption(defaultValue = "NONE") String k) {
        this.clean();
        this.insertBulk(count, putMode);

        System.out.println("generating load of " + count * percent / 100 + " read every minute");
        //generating load
        List<? extends ScheduledFuture<?>> load = IntStream.range(0, count * percent / 100)
                .mapToObj(i -> ThreadLocalRandom.current().nextLong())
                .map(i -> getKey(UUIDUtils.nameUUIDFromNamespaceAndString(UUIDUtils.NAMESPACE, i + "")))
                .map(key ->
                        executorService.scheduleAtFixedRate(() -> client.get(client.readPolicyDefault, key),
                                ThreadLocalRandom.current().nextLong((long) count * percent / 100000), 1, TimeUnit.SECONDS)
                ).toList();

        Key key = "NONE".equals(k) ? getKey(UUIDUtils.nameUUIDFromNamespaceAndString(UUIDUtils.NAMESPACE, ThreadLocalRandom.current().nextLong() + "")) : getKey(k);
        System.out.println("Getting 60 reads metrics 1 times per second");
        long acumTime = 0;
        Thread.sleep(100);
        for (int i = 0; i < 60; i++) {
            long start = System.currentTimeMillis();
            client.get(client.readPolicyDefault, key);
            acumTime += ((System.currentTimeMillis()) - start);
            Thread.sleep(1000);
        }
        System.out.printf("avgTime of %s read-ops in a %s elements namespace with a %d percent of live load is %fms %n", 60, count, percent, (double) acumTime / 60);
        load.forEach(f->f.cancel(true));
    }


    private static Key getKey(String key){
        return new Key(NAMESPACE, SET_NAME, key );
    }
}
