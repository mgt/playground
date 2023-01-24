package mad.max.aeroload.model;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class FileLineToAeroObjectsAdapter extends AsyncProducer<Pair<Key, Operation[]>> implements AsyncConsumer<Pair<String, String[]>> {
    public static final ListPolicy POLICY = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
    public static final String SET_NAME = "audience_targeting_segments";
    public static final String NAMESPACE = "tempcache";
    public static final String BIN_SEGMENT_NAME = "list";

    public FileLineToAeroObjectsAdapter(AsyncConsumer<Pair<Key, Operation[]>> consumer) {
        super(consumer);
    }

    public static Key getKey(String keyString) {
        return new Key(NAMESPACE, SET_NAME, keyString);
    }

    private static Operation[] getOperations(String... segmentsArray) {
        return Arrays.stream(segmentsArray)
                .map(com.aerospike.client.Value::get)
                .map(v -> ListOperation.append(POLICY, BIN_SEGMENT_NAME, v))
                .toArray(Operation[]::new);
    }

    @Override
    public void accept(Pair<String, String[]> pair, Observer observer) {
        try {
            this.push(new Pair<>(getKey(pair.getA()), getOperations(pair.getB())), observer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accept(Pair<String, String[]> pair) {
        this.accept(pair, null);
    }
}
