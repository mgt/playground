package mad.max.aeroload.model;

import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.AsyncConsumer;
import mad.max.aeroload.model.base.AsyncProducer;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.utils.ThreadSleepUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class FileLinesAsyncProducer extends AsyncProducer<Pair<String, String[]>> {

    public FileLinesAsyncProducer(AsyncConsumer<Pair<String, String[]>> consumer) {
        super(consumer);
    }

    public void run(Parameters parameters, FileLinesReaderConfigs configs) {
        FileLinesReaderConfigs.ReadingConfig config = configs.getReadingConfig();
        long lastReadLineNumber = -1;
        AtomicLong totalTime = new AtomicLong(0); //Keeps track of the total time spent in the process
        AtomicLong errorCount = new AtomicLong(0); //Amount of errors occurred here or down the chain
        AtomicLong okCount = new AtomicLong(0); //Amount of records successfully processed
        long startTime = System.currentTimeMillis();
        String fileName = parameters.file().getName();
        boolean fail = false;//in case there is an exception, will be used not to busy-wait

        //Maybe we need to clean previous failure reports here

        try (LineNumberReader br = new LineNumberReader(new InputStreamReader(getInputStream(parameters.file()), StandardCharsets.UTF_8))) {

            log.debug("Reading file: {} ", fileName);

            while (!br.ready()) {// wait in the case the buffer is not ready yet
                ThreadSleepUtils.sleepMaxTime();
            }

            if (config.hasHeader()) {// Skip reading 1st line of data file if it has a header
                br.readLine();
            }

            String line;
            while ((line = br.readLine()) != null && br.getLineNumber() >= parameters.start() && br.getLineNumber() <= parameters.limit()) {
                log.trace("Read line {} from file:{} ", br.getLineNumber(), fileName);
                String[] fileColumns = line.split(config.delimiter());

                Assert.isTrue((double) errorCount.get() *100 / br.getLineNumber() <= parameters.errorThreshold, ()->"Error threshold is higher than configured");
                //Validate the line we read, we should be able to get at least the segment list
                if (!StringUtils.hasText(line) || fileColumns.length < config.segmentColumnIndexInFile()) {
                    errorCount.incrementAndGet();
                    log.error("Error processing file {}: Line:{} ", fileName, br.getLineNumber());
                    continue;
                }

                String keyString = fileColumns[0];
                String[] listString = fileColumns[config.segmentColumnIndexInFile()]
                        .split(config.segmentDelimiter());

                //Configuring observers, they are going to be called async
                FileLinesAsyncObserver observe = new FileLinesAsyncObserver(okCount, errorCount, totalTime, fileName, keyString,
                        fileColumns[config.segmentColumnIndexInFile()], System.currentTimeMillis(), br.getLineNumber());
                this.push(new Pair<>(keyString, listString), observe);
                lastReadLineNumber = br.getLineNumber();
            }
        } catch (Exception e) {//Unrecoverable scenario, we don't know the nature of the error
            //given it's the last statement in the cycle probably we did not get to update
            // lastReadLineNumber and the error occurred in the next line
            log.error("Error processing file {}: Line:{} ", fileName, lastReadLineNumber + 1, e);
            fail = true;
        } finally {
            //In case there are elements being processed we wait
            while (okCount.get() + errorCount.get() < lastReadLineNumber && !fail) {
                try {
                    ThreadSleepUtils.sleepMaxTime();
                } catch (InterruptedException e) {
                    //should we interrupt up the chain too?
                    fail = true;
                }
            }
            log.info("Process finished. Lines:{} errors:{} total-time:{} avg-time/opp:{}",
                    lastReadLineNumber, errorCount.get(),
                    System.currentTimeMillis() - startTime, (double) totalTime.get() / lastReadLineNumber);
        }
    }

    private static InputStream getInputStream(File file) throws IOException {
        return Files.newInputStream(file.toPath());
    }

    public record Parameters(File file, long start, long limit, long errorThreshold) {
    }

}

