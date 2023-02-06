package mad.max.aeroload.model.transformer;

import lombok.SneakyThrows;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.N4ple;
import mad.max.aeroload.model.base.Triad;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.base.AsyncProducer;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import mad.max.aeroload.model.producer.base.LinesReaderConfigs;
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
public class InputStreamToLines extends AsyncProducer<Triad<String, String[], String>>
        implements AsyncConsumer<N4ple<InputStreamMeta, LinesReadingParameters, LinesReaderConfigs, InputStream>> {

    //No multiple extension in java, so implement + Delegate
    @Delegate
    private final AsyncConsumer<N4ple<InputStreamMeta, LinesReadingParameters, LinesReaderConfigs, InputStream>> consumerImpl;

    public InputStreamToLines(AsyncConsumer<Triad<String, String[], String>> producersConsumer) {
        super(producersConsumer);
        this.consumerImpl = new InputStreamConsumerDelegator();

    }

    class InputStreamConsumerDelegator implements AsyncConsumer<N4ple<InputStreamMeta, LinesReadingParameters, LinesReaderConfigs, InputStream>> {

        @SneakyThrows
        @Override
        public void accept(N4ple<InputStreamMeta, LinesReadingParameters, LinesReaderConfigs, InputStream> product, Observer observer) {
            LinesReaderConfigs.ReadingConfig config = product.c().getReadingConfig();
            LinesReadingParameters parameters = product.b();
            String fileName = product.a().fileName();
            long lastReadLineNumber = -1;
            AtomicLong totalTime = new AtomicLong(0); //Keeps track of the total time spent in the process
            AtomicLong errorCount = new AtomicLong(0); //Amount of errors occurred here or down the chain
            AtomicLong okCount = new AtomicLong(0); //Amount of records successfully processed
            long startTime = System.currentTimeMillis();
            boolean fail = false;//in case there is an exception, will be used not to busy-wait

            long contentLength = product.a().contentLength();
            //Maybe we need to clean previous failure reports here
            CountingInputStream cis = new CountingInputStream(product.d());
            InputStream is = cis;
            if(product.c().getReadingConfig().isCompressed())
                is= new GZIPInputStream(is);

            try (LineNumberReader br = new LineNumberReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

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

                    final long totalProcessed = Math.max(errorCount.get() + okCount.get(), 1);
                    final long bytesRead = cis.getByteCount();
                    final long totalErrors = errorCount.get();
                    final double bytesReadInErrors =  (double) totalErrors * bytesRead / totalProcessed;
                    final double threshold = bytesReadInErrors / contentLength * 100;
                    Assert.isTrue(parameters.errorThreshold() > threshold, () -> "Current threshold %f is higher than configured %d".formatted( threshold, parameters.errorThreshold()));
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
                    InputStreamToLinesObserver observe = new InputStreamToLinesObserver(okCount, errorCount, totalTime, fileName, keyString,
                            fileColumns[config.segmentColumnIndexInFile()], System.currentTimeMillis(), br.getLineNumber());
                    InputStreamToLines.this.push(new Triad<>(keyString, listString, parameters.function().apply(fileName) ), observe);
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

