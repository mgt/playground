package mad.max.aeroload.model;

import org.springframework.util.Assert;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SpinneableTask implements Runnable, Closeable {
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Thread thread = null;


    public void spinTask() {
        Assert.isTrue(!isStarted(), "Should not be started");
        this.thread = new Thread(this);
        this.started.getAndSet(true);
        this.thread.start();
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
    public void close(){
        setFinished();
        try {
            Thread.sleep(10);
            if (thread.isAlive())
                this.thread.interrupt();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
