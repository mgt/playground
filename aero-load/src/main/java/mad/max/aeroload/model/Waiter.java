package mad.max.aeroload.model;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Waiter {
    public static final int THREAD_SLEEP_MAX = 1000;
    public static final int THREAD_SLEEP_MIN = 100;

    public void busyWait() throws InterruptedException {
        Thread.sleep(THREAD_SLEEP_MAX);
    }

    public void busyWait(long sleepingTime) throws InterruptedException {
        Thread.sleep(sleepingTime == 0 ? THREAD_SLEEP_MIN : Math.min(sleepingTime, THREAD_SLEEP_MIN));
    }
}
