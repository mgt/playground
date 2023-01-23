package mad.max.aeroload.model;

public class ThreadSleepUtils {
    public static final int THREAD_SLEEP_MAX = 1000;
    public static final int THREAD_SLEEP_MIN = 100;

    public static void sleepMaxTime() throws InterruptedException {
        Thread.sleep(THREAD_SLEEP_MAX);
    }

    public static void sleepWithNonZeroMin(long sleepingTime) throws InterruptedException {
        Thread.sleep(sleepingTime == 0 ? THREAD_SLEEP_MIN : Math.min(sleepingTime, THREAD_SLEEP_MIN));
    }

    public static void sleepMinTime() throws InterruptedException {
        Thread.sleep(THREAD_SLEEP_MIN);
    }
}
