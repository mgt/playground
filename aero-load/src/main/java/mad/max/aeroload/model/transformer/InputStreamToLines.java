package mad.max.aeroload.model.transformer;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.base.AsyncProducer;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import mad.max.aeroload.utils.ThreadSleepUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

@Slf4j
public class InputStreamToLines extends AsyncProducer<Pair<String, String>>
        implements AsyncConsumer<Pair<InputStreamMeta, InputStream>> {

    //No multiple extension in java, so implement + Delegate
    @Delegate
    private final AsyncConsumer<Pair<InputStreamMeta, InputStream>> consumerImpl;

    public InputStreamToLines(AsyncConsumer<Pair<String, String>> producersConsumer, LinesReadingParameters linesReadingParameters) {
        super(producersConsumer);
        this.consumerImpl = new InputStreamConsumerDelegator(linesReadingParameters);
    }


    @AllArgsConstructor
    class InputStreamConsumerDelegator implements AsyncConsumer<Pair<InputStreamMeta, InputStream>> {
        private final LinesReadingParameters linesReadingParameters;

        @SneakyThrows
        @Override
        public void accept(Pair<InputStreamMeta, InputStream> product, Observer observer) {
            String fileName = product.a().fileName();
            long lastReadLineNumber = -1;
            AtomicLong totalTime = new AtomicLong(0); //Keeps track of the total time spent in the process
            AtomicLong errorCount = new AtomicLong(0); //Amount of errors occurred here or down the chain
            AtomicLong okCount = new AtomicLong(0); //Amount of records successfully processed
            long startTime = System.currentTimeMillis();
            boolean fail = false;//in case there is an exception, will be used not to busy-wait

            long contentLength = product.a().contentLength();
            //Maybe we need to clean previous failure reports here
            CountingInputStream cis = new CountingInputStream(product.b());
            InputStream is = cis;
            if (linesReadingParameters.compressed())
                is = new GZIPInputStream(is);

            try (LineNumberReader br = new LineNumberReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

                log.debug("Reading file: {} ", fileName);

                while (!br.ready()) {// wait in the case the buffer is not ready yet
                    ThreadSleepUtils.sleepMaxTime();
                }

                if (linesReadingParameters.hasHeader()) {// Skip reading 1st line of data file if it has a header
                    br.readLine();
                }

                String line;
                while ((line = br.readLine()) != null && br.getLineNumber() >= linesReadingParameters.start() && br.getLineNumber() <= linesReadingParameters.limit()) {
                    log.trace("Read line {} from file:{} ", br.getLineNumber(), fileName);

                    final long totalProcessed = Math.max(errorCount.get() + okCount.get(), 1);
                    final long bytesRead = cis.getByteCount();
                    final long totalErrors = errorCount.get();
                    final double bytesReadInErrors = (double) totalErrors * bytesRead / totalProcessed;
                    final double threshold = bytesReadInErrors / contentLength * 100;
                    Assert.isTrue(linesReadingParameters.errorThreshold() > threshold,
                            () -> "Current threshold %f is higher than configured %d".formatted(threshold, linesReadingParameters.errorThreshold()));
                    //Validate the line we read, we should be able to get at least the segment list
                    if (!StringUtils.hasText(line)) {
                        errorCount.incrementAndGet();
                        log.error("Error processing file {}: Line:{} ", fileName, br.getLineNumber());
                        continue;
                    }


                    //Configuring observers for downStream, they are going to be called async
                    InputStreamToLinesObserver observe = new InputStreamToLinesObserver(okCount, errorCount, totalTime, fileName,
                            line, System.currentTimeMillis(), br.getLineNumber());
                    InputStreamToLines.this.push(new Pair<>(line, fileName), observe);
                    lastReadLineNumber = br.getLineNumber();
                }
                //Because I am an AsyncConsumer, my producer gave me an observer, so my duty is calling it to notify of success processing
                if (okCount.get() + errorCount.get() == lastReadLineNumber)
                    observer.onSuccess();
            } catch (Exception e) {//Unrecoverable scenario
                //given it's the last statement in the cycle probably we did not get to update
                // lastReadLineNumber and the error occurred in the next line
                log.error("Error processing file {}: Line:{} ", fileName, lastReadLineNumber + 1, e);
                fail = true;

                //Because I am an AsyncConsumer, my producer gave me an observer, so my duty is calling it to notify of error in processing
                observer.onFail("totalTime\terrorCount\tokCount\tlastReadLineNumber%n%d\t%d\t%d\t%s".formatted(totalTime.get(), errorCount.get(), okCount.get(), lastReadLineNumber + 1));
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
                            observer.onFail("totalTime\terrorCount\tokCount\tlastReadLineNumber%n%d%d%d%s".formatted(totalTime.get(), errorCount.get(), okCount.get(), lastReadLineNumber + 1));
                        } else {
                            observer.onSuccess(); // interrupted but managed to get all things done, so notify success
                        }
                    }
                }
                log.info("Process finished. Lines:{} errors:{} total-time:{} avg-time/opp:{}",
                        lastReadLineNumber, errorCount.get(),
                        System.currentTimeMillis() - startTime, (double) totalTime.get() / lastReadLineNumber);
            }
        }
    }
}

