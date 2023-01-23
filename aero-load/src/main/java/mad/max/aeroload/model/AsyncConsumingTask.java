package mad.max.aeroload.model;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@AllArgsConstructor
@Slf4j
public abstract class AsyncConsumingTask<T> extends SpinneableTask implements Consumer<T> {
    private final BlockingQueue<T> queue;
    protected final Waiter waiter;

    public AsyncConsumingTask(int maxQueuedElements) {
        this.queue = new ArrayBlockingQueue<>(maxQueuedElements);
        this.waiter = new Waiter();
    }

    public void run() {
        while (!isFinished()) {
            log.debug("Starting to consume");
            try {
                T take = queue.take();
                log.trace("consuming element");
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
    public void consume(T product) {
        log.trace("adding element to the queue");
        queue.put(product);
    }

    protected abstract void offer(T product) throws InterruptedException;

    protected abstract void interruptedError();

    public void waitToFinish() {
        while (!isFinished() && !queue.isEmpty()) {
            try {
                waiter.busyWait();
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
