package mad.max.aeroload.model.consumer;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.listener.RecordListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.consumer.base.AsyncConsumingTask;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.utils.ThreadSleepUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class AerospikeAsyncOppsPerformer extends AsyncConsumingTask<Pair<Key, Operation[]>> {
    private final AerospikeClient client;
    private final Throttles throttles;
    private final Counts counts;
    private final int maxThroughput;

    public AerospikeAsyncOppsPerformer(AerospikeClient client, Throttles throttles, int maxThroughput, int maxCommands) {
        super(maxCommands);
        this.client = client;
        this.throttles = throttles;
        this.counts = new Counts(System.currentTimeMillis());
        this.maxThroughput = maxThroughput;
    }

    protected void offer(AsyncDecorator<Pair<Key, Operation[]>> product) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        EventLoops eventLoops = client.getCluster().eventLoops;


        if (counts.haltSignal.get()) {//checking if we have an indication that the server is having problems
            counts.writeQueuedCount.incrementAndGet();
            do {
                log.info("The slowdown flag is on, waiting for it to be cleared");
                ThreadSleepUtils.sleepMaxTime();
            } while (counts.haltSignal.get());
            counts.writeQueuedCount.decrementAndGet();
        }

        //Checking if we are exceeding configured throughput
        //if so we either wait for the average time between
        // the worst time a task take to complete, and it's best
        // or a minimum amount
        if (maxThroughput > 0 && exceedingThroughput()) {
            log.info("Configured throughput of {} was exceeded", maxThroughput);
            counts.writeQueuedCount.incrementAndGet();
            do {
                long sleepingTime = counts.getTaskAvgElapsedTime();
                ThreadSleepUtils.sleepWithNonZeroMin(sleepingTime);
            } while (exceedingThroughput());
            counts.writeQueuedCount.decrementAndGet();
        }

        boolean queue = true;
        int size = eventLoops.getSize();
        int eventLoopIndex = ThreadLocalRandom.current().nextInt(0, size);

        for (int i = 0; i < size; i++) {
            //Now we are searching for a free event loop starting randomly
            if (throttles.getAvailable((eventLoopIndex + i) % size) > 0) {
                eventLoopIndex = (eventLoopIndex + i) % size;
                queue = false;
                break;
            }
        }

        if (queue) { //we didn't find a free event loop, wait for the one randomly selected first
            counts.writeQueuedCount.incrementAndGet();
            log.debug("None of the {} loops are available. Waiting on:{}. Pending:{}", size, eventLoopIndex, counts.getPending());
        }

        if (throttles.waitForSlot(eventLoopIndex, 1)) { // if the loop has no space we will wait until is free. if not is assigned
            if (queue) //meaning the previous if was executed, so we have to decrease the queued metric
                counts.writeQueuedCount.decrementAndGet();
            counts.writeProcessingCount.incrementAndGet();


            //Aerospike async operate command
            AerospikeOperateListener listener = new AerospikeOperateListener(product.object().a(), product.observer(), eventLoopIndex, startTime);
            client.operate(eventLoops.get(eventLoopIndex), listener, client.writePolicyDefault, product.object().a(), product.object().b());
        }
    }

    private boolean exceedingThroughput() {
        return this.counts.getCurrentThroughput() > maxThroughput;
    }

    public String stats() {
        return counts.getStats();
    }

    private class AerospikeOperateListener implements RecordListener {
        private final Key key;
        private final int eventLoopIndex;
        private final long startTime;
        private final Observer observer;

        public AerospikeOperateListener(Key key, Observer observer, int eventLoopIndex, long startTime) {
            this.key = key;
            this.observer = observer;
            this.eventLoopIndex = eventLoopIndex;
            this.startTime = startTime;
        }

        // Write succeeded.
        @Override
        public void onSuccess(Key key, Record record) {

            throttles.addSlot(eventLoopIndex, 1);
            long endTime = System.currentTimeMillis();
            long time = endTime - startTime;
            counts.writeProcessingCount.decrementAndGet();
            counts.writeCompletedCount.incrementAndGet();
            counts.registerTime(time);
            Optional.ofNullable(observer).ifPresent(h -> CompletableFuture.runAsync(h::onSuccess));
        }

        // Error callback.
        @Override
        public void onFailure(AerospikeException e) {
            throttles.addSlot(eventLoopIndex, 1);
            log.warn("Operate failed: namespace={} set={} key={}", key.namespace, key.setName, key.userKey, e);
            counts.writeProcessingCount.decrementAndGet();
            counts.writeErrors.incrementAndGet();
            boolean haltSignal = false;
            switch (e.getResultCode()) {
                case 5 -> {
                    counts.writeKeyExists.incrementAndGet();
                }
                case 9 -> {
                    counts.writeTimeouts.incrementAndGet();
                    haltSignal = true;
                }
                case 13 -> { //AS_ERR_RECORD_TOO_BIG:
                    counts.writeRecordTooBig.incrementAndGet();
                }
                case 14 -> {//AS_ERR_KEY_BUSY
                    counts.writeHotKey.incrementAndGet();
                    haltSignal = true;
                }
                case 18 -> {
                    counts.writeDeviceOverload.incrementAndGet();
                    haltSignal = true;
                }
                default -> {
                    haltSignal = true;
                }
            }

            if (haltSignal && !counts.haltSignal.get()) {
                //change the slowDownFlag and what's the mechanism that will clean that flag
                // Your application should respond by delaying commands in a non-event loop thread until eventLoop.getQueueSize() is sufficiently low.
                counts.haltSignal.set(true);
                log.warn("Error received signify load on aerospike server. Will wait for {} {}", 1, TimeUnit.SECONDS, e);
                CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() -> {
                            log.info("Restarting operations after {} {}", 1, TimeUnit.SECONDS);
                            counts.haltSignal.set(false);
                        }
                );

            }
            if (Objects.nonNull(observer))
                CompletableFuture.runAsync(() -> observer.onFail(ResultCode.getResultString(e.getResultCode())));
        }
    }

    @RequiredArgsConstructor
    private static class Counts {
        private static final java.text.SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private final AtomicInteger writeQueuedCount = new AtomicInteger();
        private final AtomicInteger writeProcessingCount = new AtomicInteger();
        private final AtomicInteger writeCompletedCount = new AtomicInteger();
        private final AtomicInteger writeErrors = new AtomicInteger();
        private final AtomicInteger writeTimeouts = new AtomicInteger();
        private final AtomicInteger writeKeyExists = new AtomicInteger();
        private final AtomicInteger writeHotKey = new AtomicInteger();
        private final AtomicInteger writeDeviceOverload = new AtomicInteger();
        public AtomicLong writeRecordTooBig = new AtomicLong();
        private final AtomicLong writeBestTime = new AtomicLong();
        private final AtomicLong writeWorstTime = new AtomicLong();
        private final AtomicBoolean haltSignal = new AtomicBoolean(false);
        private final long startTime;

        public void registerTime(long time) {
            writeBestTime.getAndAccumulate(time, Math::min);
            writeWorstTime.getAndAccumulate(time, Math::max);
        }

        public long getCurrentThroughput() {
            long transactions = this.writeProcessingCount.get()
                    + this.writeQueuedCount.get()
                    + this.writeErrors.get()
                    + this.writeCompletedCount.get();
            long elapsedTime = getElapsedTime();
            return elapsedTime > 0L ? transactions / elapsedTime : 0;
        }

        public long getTaskAvgElapsedTime() {
            return (writeBestTime.get() + writeWorstTime.get()) / 2;
        }

        public String getStats() {
            // Elapsed time in ms
            long elapsedTime = getElapsedTime();
            // Transaction per second
            double tps = 1000 * (this.writeCompletedCount.get() + this.writeErrors.get()) / (double) elapsedTime;

            return String.format("StartedAt:%s (duration:%f(min) processed:%d inProgress:%d queued:%d) " +
                            "Write(throughput:%dops/ms tps:%f/s bestTime:%d worstTime:%d) " +
                            "Errors(total:%d, timeouts:%d, keyExists:%d, hotKeys:%d, deviceOverload:%d, recordTooBig:%d)",
                    DATE_FORMAT.format(new Date(startTime)), ((double) elapsedTime) / 60000,
                    this.writeCompletedCount.get(), this.writeProcessingCount.get(), this.writeQueuedCount.get(),
                    getCurrentThroughput(), tps, this.writeBestTime.get(), this.writeWorstTime.get(),
                    this.writeErrors.get(), this.writeTimeouts.get(), this.writeKeyExists.get(),
                    this.writeHotKey.get(), this.writeDeviceOverload.get(), this.writeRecordTooBig.get());
        }

        public long getPending() {
            return this.writeProcessingCount.get() + this.writeQueuedCount.get();
        }

        private long getElapsedTime() {
            return System.currentTimeMillis() - this.startTime;
        }
    }
}
