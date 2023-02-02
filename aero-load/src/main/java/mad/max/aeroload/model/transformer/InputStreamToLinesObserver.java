package mad.max.aeroload.model.transformer;

import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public record InputStreamToLinesObserver(AtomicLong okCount, AtomicLong errorCount, AtomicLong totalTime, String file,
                                         String keyString, String fileColumn, long tick,
                                         long lineNumber) implements AsyncConsumer.Observer {
    public void onFail(String error) {
        long timeSpentOnOperate = System.currentTimeMillis() - tick;
        totalTime.addAndGet(timeSpentOnOperate);
        errorCount.getAndIncrement();
        String f = String.format("%s\t%s\t%s\t%s%n", keyString, lineNumber, fileColumn, error);
        log.error("Error processing file {}. {} ", file, f);

        writeReport(f);
    }

    private void writeReport(String f) {
        try (Writer wr = new FileWriter(file + ".error", true)) {
            wr.write(f);
        } catch (IOException e) {//Sorry, nothing we can do about it.
            log.warn("Cannot write report to \"{}.error\"", file, e);
        }
    }

    public void onSuccess() {
        long timeSpentOnOperate = System.currentTimeMillis() - tick;
        totalTime.accumulateAndGet(timeSpentOnOperate, Math::addExact);
        okCount.incrementAndGet();
        String format = String.format("inserted record for file %s key:%s. took %d. Total inserted so far (%d/%d). avg time %f",
                file, keyString, timeSpentOnOperate, okCount.get(), lineNumber, (double) totalTime.get() / lineNumber);
        log.trace(format);
        if (lineNumber % 1000 == 0) {
            log.info(format);
        }
    }
}
