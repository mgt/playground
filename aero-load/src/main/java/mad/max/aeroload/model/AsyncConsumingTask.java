package mad.max.aeroload.model;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.utils.ThreadSleepUtils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

@AllArgsConstructor
@Slf4j
public abstract class AsyncConsumingTask<T> extends SpinOffTask implements Consumer<T> {
    private final BlockingQueue<T> queue;

    public AsyncConsumingTask(int maxQueuedElements) {
        this.queue = new ArrayBlockingQueue<>(maxQueuedElements);
    }

    public void run() {
        while (!isFinished()) {
            try {
                T take = queue.take();
                this.offer(take);
            } catch (InterruptedException e) {
                if (queue.size() == 0)
                    setFinished();
                else {
                    this.interruptedError();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @SneakyThrows
    public void accept(T product) {
        log.trace("adding element to the queue");
        queue.put(product);
    }

    protected abstract void offer(T product) throws InterruptedException;

    protected abstract void interruptedError();

    public void waitToFinish() {
        while (!isFinished() && !queue.isEmpty()) {
            try {
                ThreadSleepUtils.sleepMaxTime();
            } catch (InterruptedException e) {
                if (!queue.isEmpty()) {
                    log.error("did not finish but asked to stop", e);
                    throw new RuntimeException(e);
                }
            }
        }
        super.close();
    }


}
