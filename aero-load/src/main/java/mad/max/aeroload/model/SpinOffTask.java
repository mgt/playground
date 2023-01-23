package mad.max.aeroload.model;

import org.springframework.util.Assert;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static mad.max.aeroload.utils.ThreadSleepUtils.sleepMinTime;

/**
 * A runnable that can spin off the run method in another thread.
 * Run method should address changes on the finished flag and end its processing.
 */
public abstract class SpinOffTask implements Runnable, Closeable {
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private CompletableFuture<Void> future;

    public void spinOff() {
        Assert.isTrue(!isStarted(), "Should not be started");
        future = CompletableFuture.runAsync(this);
        this.started.getAndSet(true);
    }

    public void setFinished() {
        Assert.isTrue(isStarted(), "Should be started first");
        this.finished.getAndSet(true);
    }

    public boolean isFinished() {
        return this.finished.get();
    }

    public boolean isStarted() {
        return this.started.get();
    }

    @Override
    public void close() {
        setFinished(); //set the finished signal
        try {
            sleepMinTime(); //giving it time to ack the finish signal
            future.cancel(true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
