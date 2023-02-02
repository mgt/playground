package mad.max.aeroload.model.transformer;

import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Triad;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.consumer.base.AsyncConsumingTask;
import mad.max.aeroload.model.producer.FileLinesAsyncObserver;
import mad.max.aeroload.model.producer.FileLinesReaderConfigs;
import mad.max.aeroload.model.producer.base.AsyncProducer;
import mad.max.aeroload.model.transformer.base.InputStreamMeta;
import mad.max.aeroload.utils.ThreadSleepUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class InputStreamToFileLinesAdapter extends AsyncProducer<Triad<String, String[], String>> implements AsyncConsumer<Triad<InputStreamMeta, FileLinesReaderConfigs, InputStream>> {

    //No multiple extension in java, so implement + Delegate
    @Delegate
    private final AsyncConsumingTask<Triad<InputStreamMeta, FileLinesReaderConfigs, InputStream>> consumerImpl;

    public InputStreamToFileLinesAdapter(AsyncConsumer<Triad<String, String[], String>> producersConsumer) {
        super(producersConsumer);
        this.consumerImpl = new S3InputConsumerDelegator();
    }


    class S3InputConsumerDelegator extends AsyncConsumingTask<Triad<InputStreamMeta, FileLinesReaderConfigs, InputStream>> {


        public S3InputConsumerDelegator() {
            super(1); //Allow to process one file at the time
        }

        @Override
        protected void offer(AsyncDecorator<Triad<InputStreamMeta, FileLinesReaderConfigs, InputStream>> decorator) {
            Triad<InputStreamMeta, FileLinesReaderConfigs, InputStream> triad = decorator.object();
            FileLinesReaderConfigs.ReadingConfig config = triad.b().getReadingConfig();
            InputStreamMeta parameters = triad.a();
            String fileName = parameters.fileName();
            long lastReadLineNumber = -1;
            AtomicLong totalTime = new AtomicLong(0); //Keeps track of the total time spent in the process
            AtomicLong errorCount = new AtomicLong(0); //Amount of errors occurred here or down the chain
            AtomicLong okCount = new AtomicLong(0); //Amount of records successfully processed
            long startTime = System.currentTimeMillis();
            boolean fail = false;//in case there is an exception, will be used not to busy-wait

            //Maybe we need to clean previous failure reports here

            try (LineNumberReader br = new LineNumberReader(new InputStreamReader(triad.c(), StandardCharsets.UTF_8))) {

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

                    Assert.isTrue((double) errorCount.get() * 100 / br.getLineNumber() <= parameters.errorThreshold(), () -> "Error threshold is higher than configured");
                    //Validate the line we read, we should be able to get at least the segment list
                    if (!StringUtils.hasText(line) || fileColumns.length < config.segmentColumnIndexInFile()) {
                        errorCount.incrementAndGet();
                        log.error("Error processing file {}: Line:{} ", fileName, br.getLineNumber());
                        continue;
                    }

                    String keyString = fileColumns[0];
                    String[] listString = fileColumns[config.segmentColumnIndexInFile()]
                            .split(config.segmentDelimiter());

                    //Configuring observers for downStream, they are going to be called async
                    FileLinesAsyncObserver observe = new FileLinesAsyncObserver(okCount, errorCount, totalTime, fileName, keyString,
                            fileColumns[config.segmentColumnIndexInFile()], System.currentTimeMillis(), br.getLineNumber());
                    InputStreamToFileLinesAdapter.this.push(new Triad<>(keyString, listString, parameters.binName()), observe);
                    lastReadLineNumber = br.getLineNumber();
                }
                //Because I am an AsyncConsumer, my producer gave me an observer, so my duty is calling it to notify of success processing
                if (okCount.get() + errorCount.get() == lastReadLineNumber)
                    decorator.observer().onSuccess();
            } catch (Exception e) {//Unrecoverable scenario, we don't know the nature of the error
                //given it's the last statement in the cycle probably we did not get to update
                // lastReadLineNumber and the error occurred in the next line
                log.error("Error processing file {}: Line:{} ", fileName, lastReadLineNumber + 1, e);
                fail = true;

                //Because I am an AsyncConsumer, my producer gave me an observer, so my duty is calling it to notify of error in processing
                decorator.observer().onFail("totalTime\terrorCount\tokCount\tlastReadLineNumber%n%d%d%d%s".formatted(totalTime.get(), errorCount.get(), okCount.get(), lastReadLineNumber + 1));
            } finally {
                //In case there are elements being processed we wait
                while (okCount.get() + errorCount.get() < lastReadLineNumber && !fail) {
                    try {
                        ThreadSleepUtils.sleepMaxTime();
                    } catch (InterruptedException e) {
                        //should we interrupt up the chain too?
                        if (okCount.get() + errorCount.get() < lastReadLineNumber) { //if after all loops this is still true then we fail to process the total amount
                            fail = true;
                            // we know we have to notify failure
                            decorator.observer().onFail("totalTime\terrorCount\tokCount\tlastReadLineNumber%n%d%d%d%s".formatted(totalTime.get(), errorCount.get(), okCount.get(), lastReadLineNumber + 1));
                        } else {
                            decorator.observer().onSuccess(); // interrupted but managed to get all things done, so notify success
                        }
                    }
                }
                log.info("Process finished. Lines:{} errors:{} total-time:{} avg-time/opp:{}",
                        lastReadLineNumber, errorCount.get(),
                        System.currentTimeMillis() - startTime, (double) totalTime.get() / lastReadLineNumber);
            }
        }
    }

    ;
}

