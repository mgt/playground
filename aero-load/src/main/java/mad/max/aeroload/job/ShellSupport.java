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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static mad.max.aeroload.model.FileLineToAeroObjectsAdapter.BIN_SEGMENT_NAME;
import static mad.max.aeroload.model.FileLineToAeroObjectsAdapter.NAMESPACE;
import static mad.max.aeroload.model.FileLineToAeroObjectsAdapter.POLICY;
import static mad.max.aeroload.model.FileLineToAeroObjectsAdapter.SET_NAME;
import static mad.max.aeroload.model.FileLineToAeroObjectsAdapter.getKey;

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
    public void run(@ShellOption(defaultValue = "DEFAULT") LoadingProfile.PredefinedProfiles profile, long limit) {
        StopWatch timeMeasure = new StopWatch();
        timeMeasure.start();
        LoadingProfile p = profile.getProfile();
        loadingService.load(new LoadingProfile(p.getMaxThroughput(), limit > 0 ? limit : p.getMaxLinesPerFile(), p.getMaxParallelCommands(), p.getMaxQueuedElements()));
        timeMeasure.stop();
        System.out.println("run running time=" + timeMeasure.getLastTaskTimeMillis());
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

    @ShellMethod("operation to try limit on one key")
    public void tryLimit(String k, @ShellOption(value = "c", defaultValue = "10000") int count, @ShellOption(value = "m", defaultValue = "v") String putMode) {
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
                client.put(client.writePolicyDefault, key, new Bin(BIN_SEGMENT_NAME,
                        String.join(",", segments.subList(0, ThreadLocalRandom.current().nextInt(1, segments.size())))));
            } else {
                Operation[] operations =
                        segments.subList(0, ThreadLocalRandom.current().nextInt(1, segments.size()))
                                .stream()
                                .map(com.aerospike.client.Value::get)
                                .map(v -> ListOperation.append(POLICY, BIN_SEGMENT_NAME, v))
                                .toArray(Operation[]::new);
                client.operate(client.writePolicyDefault, key, operations);
            }

        }

        timeMeasure.stop();
        System.out.println("insert-force running time =" + timeMeasure.getLastTaskTimeMillis());
    }


    @SneakyThrows
    @ShellMethod("operation to insert an amount of records")
    public void testReads(@ShellOption(value = "c", defaultValue = "1000000") int count,
                          @ShellOption(value = "m", defaultValue = "v") String putMode,
                          @ShellOption(value = "%", defaultValue = "20") int percent,
                          @ShellOption(defaultValue = "NONE") String k) {
        this.clean();
        this.insertBulk(count, putMode);

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        System.out.println("generating load of " + count * percent / 100 + " read every minute");
        executorService.scheduleAtFixedRate(() -> {
            //generating load
            IntStream.range(0, count * percent / 100)
                    .mapToObj(i -> ThreadLocalRandom.current().nextLong())
                    .map(i -> getKey(UUIDUtils.nameUUIDFromNamespaceAndString(UUIDUtils.NAMESPACE, i + "")))
                    .forEach(key -> CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(ThreadLocalRandom.current().nextLong(1000));
                        } catch (InterruptedException e) {
                        }
                        client.get(client.readPolicyDefault, key);
                    }));
        }, 0, 1, TimeUnit.SECONDS);

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
        executorService.shutdown();

        System.out.printf("avgTime of %s read-ops in a %s elements namespace with a %d percent of live load is %fms %n", 60, count, percent, (double) acumTime / 60);
    }


}