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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static mad.max.aeroload.JobConfig.THREAD_SLEEP_MIN;

@Slf4j
public class AerospikeLoader extends Consumer<Product<Key, Operation[]>> {
    private final AerospikeClient client;
    private final Throttles throttles;
    private final long maxThroughput;
    private final AerospikeLoadingMetrics metrics;

    public AerospikeLoader(AerospikeClient client, Throttles throttles, long maxThroughput) {
        this.client = client;
        this.throttles = throttles;
        this.maxThroughput = maxThroughput;
        this.metrics = new AerospikeLoadingMetrics(System.currentTimeMillis());
    }

    @SneakyThrows
    protected void consume(Product<Key, Operation[]> product) {
        long startTime = System.currentTimeMillis();
        EventLoops eventLoops = client.getCluster().eventLoops;

        //Checking if we are exceeding configured throughput
        //if so we either wait for the average time between
        // the worst time a task take to complete and it's best
        // or a minimun amount
        if (maxThroughput > 0 && exceedingThroughput()) {
            log.debug("Configured Throughput was exceeded:{}", maxThroughput);
            metrics.writeQueuedCount.incrementAndGet();
            do {
                long sleepingTime = metrics.getTaskElapsedAverageTime();
                Thread.sleep(sleepingTime == 0 ? THREAD_SLEEP_MIN : Math.min(sleepingTime, THREAD_SLEEP_MIN));
            } while (exceedingThroughput());
            metrics.writeQueuedCount.decrementAndGet();
        }

        int eventLoopIndex = -1;
        int maxAvailable = 0;
        int size = eventLoops.getSize();

        for (int i = 0; i < size; i++) {
            //Now we are searching for a free event loop
            //an improvement here is to randomly search from both end and begining
            //so not all the first slots are filled first
            int available = throttles.getAvailable(i);
            if (maxAvailable > available) {
                eventLoopIndex = i;
                maxAvailable = available;
            }
        }

        if (maxAvailable == 0) { //we didn't find a free event loop, search one randomly
            metrics.writeQueuedCount.incrementAndGet();
            eventLoopIndex = ThreadLocalRandom.current().nextInt(0, size);
            log.debug("None of the {} loops are available. Waiting on:{}. Pending:{}", size, eventLoopIndex, metrics.getPending());
        }

        if (throttles.waitForSlot(eventLoopIndex, 1)) { // if the loop has no space we will wait until is free. if not is assigned
            if (maxAvailable == 0) //meaning we waited before continued, so we have to decrease the queued metric
                metrics.writeQueuedCount.decrementAndGet();
            metrics.writeProcessingCount.incrementAndGet();


            //Aerospike async operate command
            AerospikeLoaderWriteListener listener = new AerospikeLoaderWriteListener(product, eventLoopIndex, startTime);
            client.operate(eventLoops.get(eventLoopIndex), listener, client.writePolicyDefault, product.getA(), product.getB());
        }
    }

    @Override
    protected void interruptedError() {
        metrics.writeErrors.incrementAndGet();
    }

    private boolean exceedingThroughput() {
        return this.metrics.getCurrentThroughput() > this.maxThroughput;
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

        // Write success callback.

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
                    //case 13:
                    //Record too big
                case 14:
                    metrics.writeHotKey.incrementAndGet();
                case 18:
                    metrics.writeDeviceOverload.incrementAndGet();

                    //maybe for some other errors we need to flag to #consume method
                    // not to continue for a while, so we don't make a situation worst
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
        private final AtomicLong writeBestTime = new AtomicLong();
        private final AtomicLong writeWorstTime = new AtomicLong();
        private final long startTime;

        public void registerTime(long time) {
            writeBestTime.getAndAccumulate(time,Math::min);
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

        public long getTaskElapsedAverageTime(){
            return (writeBestTime.get() + writeWorstTime.get()) / 2;
        }

        public String getStats() {
            // Elapsed time in ms
            long elapsedTime = getElapsedTime();
            // Transaction per second
            double tps = 1000 * (this.writeCompletedCount.get() + this.writeErrors.get()) / (double) elapsedTime;

            return String.format("StartedAt:%s (duration:%f(min) processed:%d inProgress:%d queued:%d) " +
                            "Write(throughput:%d/ms tps:%f/s bestTime:%d worstTime:%d) " +
                            "Errors(total:%d, timeouts:%d, keyExists:%d, hotKeys:%d, deviceOverload:%d)",
                    DATE_FORMAT.format(new Date(startTime)), ((double) elapsedTime) / 60000,
                    this.writeCompletedCount.get(), this.writeProcessingCount.get(), this.writeQueuedCount.get(),
                    getCurrentThroughput(), tps, this.writeBestTime.get(), this.writeWorstTime.get(),
                    this.writeErrors.get(), this.writeTimeouts.get(), this.writeKeyExists.get(),
                    this.writeHotKey.get(), this.writeDeviceOverload.get());
        }

        public long getPending() {
            return this.writeProcessingCount.get() + this.writeQueuedCount.get();
        }
        private long getElapsedTime() {
            return System.currentTimeMillis() - this.startTime;
        }
    }
}
