package mad.max.aeroload.model.transformer;

import mad.max.aeroload.model.base.N4ple;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import mad.max.aeroload.model.producer.base.LinesReaderConfigs;
import mad.max.aeroload.model.producer.base.AsyncProducer;

import java.io.InputStream;

public class InputStreamParameterAdder extends AsyncProducer<N4ple<InputStreamMeta,LinesReadingMeta, LinesReaderConfigs, InputStream>> implements AsyncConsumer<Pair<InputStreamMeta, InputStream>> {

    public static final String SEGMENT_BIN_NAME = "segments";
    private final LinesReaderConfigs linesReaderConfigs;

    public InputStreamParameterAdder(LinesReaderConfigs aDefault, AsyncConsumer<N4ple<InputStreamMeta,LinesReadingMeta, LinesReaderConfigs, InputStream>> consumer) {
        super(consumer);
        linesReaderConfigs = aDefault;
    }

    @Override
    public void accept(Pair<InputStreamMeta, InputStream> product, Observer observer) {
        //We should check if the input stream is compressed
        //we should check if the input stream was processed
        //we should guess the name of the bin according to the file

        this.push(new N4ple<>(product.a(), new LinesReadingMeta(0, Long.MAX_VALUE, 10, SEGMENT_BIN_NAME), linesReaderConfigs, product.b()),observer);
    }


}
