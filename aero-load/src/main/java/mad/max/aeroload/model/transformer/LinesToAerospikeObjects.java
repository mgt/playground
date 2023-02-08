package mad.max.aeroload.model.transformer;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.consumer.base.AsyncConsumingTask;
import mad.max.aeroload.model.producer.base.AsyncProducer;
import mad.max.aeroload.utils.AeroUtils;
import org.springframework.util.Assert;

import static mad.max.aeroload.utils.AeroUtils.getAppendIfNotExistOperations;
import static org.springframework.util.StringUtils.hasText;

@Slf4j
public class LinesToAerospikeObjects extends AsyncProducer<Pair<Key, Operation[]>> implements AsyncConsumer<Pair<String,  String>> {

    @Delegate
    private final AsyncConsumingTask<Pair<String, String>> consumingTask;
    private final LinesToAerospikeParameters parameters;

    public LinesToAerospikeObjects(AsyncConsumer<Pair<Key, Operation[]>> consumer, LinesToAerospikeParameters parameters) {
        super(consumer);
        this.parameters = parameters;
        this.consumingTask = new AsyncConsumingTask<>(100) {
            @Override
            protected void offer(AsyncDecorator<Pair<String,  String>> product)  {
                Pair<String, String> pair = product.object();

                String[] fileColumns = pair.a().split(parameters.lineDelimiter());

                String keyString = fileColumns[0];
                String segmentBinName = parameters.segmentNamingFunction().apply(pair.b());
                Assert.isTrue(fileColumns.length > parameters.segmentColumnIndexInFile(), "Invalid line");
                String segments = fileColumns[parameters.segmentColumnIndexInFile()];
                Assert.isTrue(hasText(segments), "Invalid line");
                String[] segmentArray = segments.split(parameters.segmentDelimiter());
                LinesToAerospikeObjects.this.push(new Pair<>(getKey(keyString), getAppendIfNotExistOperations(segmentBinName, segmentArray)), product.observer());
            }
        };
        consumingTask.spinOff();
    }

    public Key getKey(String keyString) {
        return new Key(parameters.namespace(), parameters.setName(), keyString);
    }


}
