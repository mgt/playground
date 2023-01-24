package mad.max.aeroload.model;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.utils.ThreadSleepUtils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@AllArgsConstructor
@Slf4j
public abstract class AsyncConsumingTask<T> extends SpinOffTask implements AsyncConsumer<T> {
    private final BlockingQueue<AsyncDecorator<T>> queue;

    public AsyncConsumingTask(int maxQueuedElements) {
        this.queue = new ArrayBlockingQueue<>(maxQueuedElements);
    }

    public void run() {
        while (!isFinished()) {
            try {
                AsyncDecorator<T> take = queue.take();
                this.offer(take);
            } catch (InterruptedException e) {
                if (queue.size() == 0)
                    setFinished();
                else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @SneakyThrows
    public void accept(T product) {
        log.trace("adding element to the queue");
        queue.put(new AsyncDecorator<>(product, null));
    }

    @SneakyThrows
    public void accept(T product, Observer observer) {
        log.trace("adding element to the queue");
        queue.put(new AsyncDecorator<>(product, observer));
    }
    protected abstract void offer(AsyncDecorator<T> product) throws InterruptedException;

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


    public record AsyncDecorator<T>(T object, Observer observer) {}

}
