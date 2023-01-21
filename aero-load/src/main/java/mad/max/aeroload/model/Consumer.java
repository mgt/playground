package mad.max.aeroload.model;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.JobConfig;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static mad.max.aeroload.JobConfig.CONSUMER_CAPACITY;
import static mad.max.aeroload.JobConfig.THREAD_SLEEP_MAX;

@AllArgsConstructor
@Slf4j
public abstract class Consumer<T> extends SpinneableTask {
    private final BlockingQueue<T> queue = new ArrayBlockingQueue<>(CONSUMER_CAPACITY);

    public void run() {
        while (!isFinished()) {
            log.debug("Starting to consume");
            try {
                T take = queue.take();
                log.debug("consuming element");
                this.consume(take);
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

    public void offer(T product) throws InterruptedException {
        log.debug("adding element to the queue");
        queue.put(product);
    }

    protected abstract void consume(T product);

    protected abstract void interruptedError();

    public void waitToFinish() {
        while (!isFinished() && !queue.isEmpty()) {
            try {
                Thread.sleep(THREAD_SLEEP_MAX);
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
