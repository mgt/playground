package mad.max.aeroload.model.transformer;

import mad.max.aeroload.model.base.N4ple;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.base.AsyncProducer;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import mad.max.aeroload.model.producer.base.LinesReaderConfigs;

import java.io.InputStream;

public class InputStreamParameterAdder extends AsyncProducer<N4ple<InputStreamMeta, LinesReadingParameters, LinesReaderConfigs, InputStream>>
        implements AsyncConsumer<Pair<InputStreamMeta, InputStream>> {

    private final LinesReaderConfigs linesReaderConfigs;
    private final LinesReadingParameters linesReaderParams;

    public InputStreamParameterAdder(LinesReaderConfigs config, String segmentsBinName, int errorThreshold, AsyncConsumer<N4ple<InputStreamMeta, LinesReadingParameters, LinesReaderConfigs, InputStream>> consumer) {
        super(consumer);
        this.linesReaderConfigs = config;
        this.linesReaderParams=  new LinesReadingParameters( 0, Long.MAX_VALUE, errorThreshold, fn-> segmentsBinName);
    }

    public InputStreamParameterAdder(LinesReaderConfigs config, LinesReadingParameters parameters, AsyncConsumer<N4ple<InputStreamMeta, LinesReadingParameters, LinesReaderConfigs, InputStream>> consumer) {
        super(consumer);
        this.linesReaderConfigs = config;
        this.linesReaderParams=  parameters;
    }
    @Override
    public void accept(Pair<InputStreamMeta, InputStream> product, Observer observer) {
        this.push(new N4ple<>(product.a(),linesReaderParams, linesReaderConfigs, product.b()), observer);
    }


}
