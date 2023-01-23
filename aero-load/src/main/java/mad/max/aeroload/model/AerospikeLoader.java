package mad.max.aeroload.model;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.listener.RecordListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.utils.ThreadSleepUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class AerospikeLoader extends AsyncConsumingTask<Product<Key, Operation[]>> {
    private final AerospikeClient client;
    private final Throttles throttles;
    private final AerospikeLoadingMetrics metrics;
    private final int maxThroughput;


    public AerospikeLoader(AerospikeClient client, Throttles throttles, int maxThroughput, int maxCommands) {
        super(maxCommands);
        this.client = client;
        this.throttles = throttles;
        this.metrics = new AerospikeLoadingMetrics(System.currentTimeMillis());
        this.maxThroughput = maxThroughput;
    }

    protected void offer(Product<Key, Operation[]> product) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        EventLoops eventLoops = client.getCluster().eventLoops;


        if (metrics.slowDownFlag.get()) {//checking if we have an indication that the server is having problems
            metrics.writeQueuedCount.incrementAndGet();
            do {
                log.info("The slowdown flag is on, waiting for it to be cleared");
                ThreadSleepUtils.sleepMaxTime();
            } while (metrics.slowDownFlag.get());
            metrics.writeQueuedCount.decrementAndGet();
        }

        //Checking if we are exceeding configured throughput
        //if so we either wait for the average time between
        // the worst time a task take to complete, and it's best
        // or a minimum amount
        if (maxThroughput > 0 && exceedingThroughput()) {
            log.info("Configured throughput of {} was exceeded", maxThroughput);
            metrics.writeQueuedCount.incrementAndGet();
            do {
                long sleepingTime = metrics.getTaskAvgElapsedTime();
                ThreadSleepUtils.sleepWithNonZeroMin(sleepingTime);
            } while (exceedingThroughput());
            metrics.writeQueuedCount.decrementAndGet();
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
            metrics.writeQueuedCount.incrementAndGet();
            log.debug("None of the {} loops are available. Waiting on:{}. Pending:{}", size, eventLoopIndex, metrics.getPending());
        }

        if (throttles.waitForSlot(eventLoopIndex, 1)) { // if the loop has no space we will wait until is free. if not is assigned
            if (queue) //meaning the previous if was executed, so we have to decrease the queued metric
                metrics.writeQueuedCount.decrementAndGet();
            metrics.writeProcessingCount.incrementAndGet();


            //Aerospike async operate command
            AerospikeLoaderWriteListener listener = new AerospikeLoaderWriteListener(product, eventLoopIndex, startTime);
            client.operate(eventLoops.get(eventLoopIndex), listener, client.writePolicyDefault, product.getA(), product.getB());
        }
    }

    @Override
    protected void interruptedError() {
    }

    private boolean exceedingThroughput() {
        return this.metrics.getCurrentThroughput() > maxThroughput;
    }

    public String stats() {
        return metrics.getStats();
    }

    private class AerospikeLoaderWriteListener implements RecordListener {
        private final Key key;
        private final int eventLoopIndex;
        private final long startTime;
        private final Runnable onSuccess;
        private final Runnable onFailure;

        public AerospikeLoaderWriteListener(Product<Key, Operation[]> p, int eventLoopIndex, long startTime) {
            this.key = p.getA();
            this.onSuccess = p.getSuccessHandler();
            this.onFailure = p.getFailureHandler();
            this.eventLoopIndex = eventLoopIndex;
            this.startTime = startTime;
        }

        // Write succeeded.
        @Override
        public void onSuccess(Key key, Record record) {

            throttles.addSlot(eventLoopIndex, 1);
            long endTime = System.currentTimeMillis();
            long time = endTime - startTime;
            metrics.writeProcessingCount.decrementAndGet();
            metrics.writeCompletedCount.incrementAndGet();
            metrics.registerTime(time);
            CompletableFuture.runAsync(onSuccess);
        }

        // Error callback.
        public void onFailure(AerospikeException e) {
            throttles.addSlot(eventLoopIndex, 1);
            log.warn("Operate failed: namespace={} set={} key={}", key.namespace, key.setName, key.userKey, e);
            metrics.writeProcessingCount.decrementAndGet();
            metrics.writeErrors.incrementAndGet();
            switch (e.getResultCode()) {
                case 5:
                    metrics.writeKeyExists.incrementAndGet();
                case 9:
                    metrics.writeTimeouts.incrementAndGet();
                case 13: //AS_ERR_RECORD_TOO_BIG:
                    metrics.writeRecordTooBig.incrementAndGet();
                case 14: //AS_ERR_KEY_BUSY
                    metrics.writeHotKey.incrementAndGet();
                case 18:
                    metrics.writeDeviceOverload.incrementAndGet();

                    //TODO identify which errors should change the slowDownFlag and what's the mechanism that will clean that flag
                    //should consider that other threads might rise the flag too.
                    //AerospikeException.AsyncQueueFull exception is thrown.
                    // Your application should respond by delaying commands in a non-event loop thread until eventLoop.getQueueSize() is sufficiently low.
            }
            CompletableFuture.runAsync(onFailure);
        }
    }

    @RequiredArgsConstructor
    private static class AerospikeLoadingMetrics {
        private static final java.text.SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private final AtomicInteger writeQueuedCount = new AtomicInteger();
        private final AtomicInteger writeProcessingCount = new AtomicInteger();
        private final AtomicInteger writeCompletedCount = new AtomicInteger();
        private final AtomicInteger writeErrors = new AtomicInteger();
        private final AtomicInteger writeTimeouts = new AtomicInteger();
        private final AtomicInteger writeKeyExists = new AtomicInteger();
        private final AtomicInteger writeHotKey = new AtomicInteger();
        private final AtomicInteger writeDeviceOverload = new AtomicInteger();
        public AtomicLong writeRecordTooBig =  new AtomicLong();
        private final AtomicLong writeBestTime = new AtomicLong();
        private final AtomicLong writeWorstTime = new AtomicLong();
        private final AtomicBoolean slowDownFlag = new AtomicBoolean(false);
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
