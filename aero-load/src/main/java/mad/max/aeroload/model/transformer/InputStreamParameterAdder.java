package mad.max.aeroload.model.transformer;

import mad.max.aeroload.model.base.Triad;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.LinesReaderConfigs;
import mad.max.aeroload.model.producer.base.AsyncProducer;

import java.io.InputStream;

public class InputStreamParameterAdder extends AsyncProducer<Triad<InputStreamMeta, LinesReaderConfigs, InputStream>> implements AsyncConsumer<Triad<String, String, InputStream>> {

    public static final String SEGMENT_BIN_NAME = "segments";
    private final LinesReaderConfigs linesReaderConfigs;

    public InputStreamParameterAdder(LinesReaderConfigs aDefault, AsyncConsumer<Triad<InputStreamMeta, LinesReaderConfigs, InputStream>> consumer) {
        super(consumer);
        linesReaderConfigs = aDefault;
    }

    @Override
    public void accept(Triad<String, String, InputStream> product, Observer observer) {
        //We should check if the input stream is compressed
        //we should check if the input stream was processed
        //we should guess the name of the bin according to the file

        this.push(new Triad<>(new InputStreamMeta(product.a(), 0, Long.MAX_VALUE, 10, SEGMENT_BIN_NAME), linesReaderConfigs, product.c()),observer);
    }


}
