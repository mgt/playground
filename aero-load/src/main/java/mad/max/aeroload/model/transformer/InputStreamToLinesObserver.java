package mad.max.aeroload.model.transformer;

import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public record InputStreamToLinesObserver(AtomicLong okCount, AtomicLong errorCount, AtomicLong totalTime, String file,
                                         String keyString, String fileColumn, long tick,
                                         long lineNumber) implements AsyncConsumer.Observer {
    public void onFail(String error) {
        long timeSpentOnOperate = System.currentTimeMillis() - tick;
        totalTime.addAndGet(timeSpentOnOperate);
        errorCount.getAndIncrement();
        String f = String.format("%d\t%s\t%s\t%s%n", lineNumber, keyString, fileColumn, error);
        log.error("Error processing file {}. {} ", file, f);

        writeReport(f);
    }

    private void writeReport(String f) {

        try{
            Files.writeString(Path.of(FilenameUtils.getPath(file), FilenameUtils.getBaseName(file) + ".retry"), f, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        } catch (IOException e) {//Sorry, nothing we can do about it.
            log.warn("Cannot write report to \"{}.retry\"", FilenameUtils.getBaseName(file), e);
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
