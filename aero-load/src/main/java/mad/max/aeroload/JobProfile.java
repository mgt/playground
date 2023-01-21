package mad.max.aeroload;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class JobProfile {

    public static final int CAPACITY = 50;
    public static final int CONSUMER_CAPACITY = 200;
    public static final int THREAD_SLEEP_MAX = 1000;
    public static final int THREAD_SLEEP_MIN = 100;
    private int maxThroughput;

    public int getMaxParallelCommands() {
        return CAPACITY;
    }

    public int getMaxQueuedElements() {
        return CONSUMER_CAPACITY;
    }


    public void busyWait() throws InterruptedException {
        Thread.sleep(THREAD_SLEEP_MAX);
    }

    public void busyWait(long sleepingTime) throws InterruptedException {
        Thread.sleep(sleepingTime == 0 ? THREAD_SLEEP_MIN : Math.min(sleepingTime, THREAD_SLEEP_MIN));
    }


    public int getMaxThroughput() {
        return maxThroughput;
    }
}
