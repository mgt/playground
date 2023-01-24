package mad.max.aeroload.model;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.AsyncConsumer;
import mad.max.aeroload.model.base.AsyncProducer;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.base.Triad;

import java.util.Arrays;

@Slf4j
public class FileLinesToAerospikeAdapter extends AsyncProducer<Pair<Key, Operation[]>> implements AsyncConsumer<Triad<String, String[],String>> {
    public static final ListPolicy POLICY = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
    public static final String SET_NAME = "audience_targeting_segments";
    public static final String NAMESPACE = "tempcache";

    public FileLinesToAerospikeAdapter(AsyncConsumer<Pair<Key, Operation[]>> consumer) {
        super(consumer);
    }

    public static Key getKey(String keyString) {
        return new Key(NAMESPACE, SET_NAME, keyString);
    }

    private static Operation[] getOperations(String binName, String... segmentsArray) {
        return Arrays.stream(segmentsArray)
                .map(com.aerospike.client.Value::get)
                .map(v -> ListOperation.append(POLICY, binName, v))
                .toArray(Operation[]::new);
    }

    @Override
    public void accept(Triad<String, String[],String> triad, Observer observer) {
        this.push(new Pair<>(getKey(triad.getA()), getOperations(triad.getC(), triad.getB())), observer);
    }

    @Override
    public void accept(Triad<String, String[],String> triad) {
        this.accept(triad, null);
    }
}
