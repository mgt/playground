package mad.max.aeroload.model.transformer;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.base.Triad;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.consumer.base.AsyncConsumingTask;
import mad.max.aeroload.model.producer.base.AsyncProducer;

@Slf4j
public class LinesToAerospikeObjects extends AsyncProducer<Pair<Key, Operation[]>> implements AsyncConsumer<Triad<String, String[], String>> {
    public static final ListPolicy POLICY = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
    public static final String SET_NAME = "audience_targeting_segments";
    public static final String NAMESPACE = "tempcache";

    @Delegate
    private final AsyncConsumingTask<Triad<String, String[], String>> consumingTask;

    public LinesToAerospikeObjects(AsyncConsumer<Pair<Key, Operation[]>> consumer) {
        super(consumer);
        this.consumingTask = new AsyncConsumingTask<>(100) {
            @Override
            protected void offer(AsyncDecorator<Triad<String, String[], String>> product)  {
                Triad<String, String[], String> triad = product.object();
                LinesToAerospikeObjects.this.push(new Pair<>(getKey(triad.a()), getOperations(triad.c(), triad.b())), product.observer());

            }
        };
        consumingTask.spinOff();
    }

    public static Key getKey(String keyString) {
        return new Key(NAMESPACE, SET_NAME, keyString);
    }

    //This method gets invoked alot, so imperative paradig. for performance
    private static Operation[] getOperations(String binName, String... segmentsArray) {
        Operation[] ops = new Operation[segmentsArray.length];
        for (int i = 0; i < segmentsArray.length; i++) {
            ops[i] = ListOperation.append(POLICY, binName, Value.get(segmentsArray[i]));
        }
        return ops;
    }



}
