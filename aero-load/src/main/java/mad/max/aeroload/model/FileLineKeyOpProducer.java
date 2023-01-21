package mad.max.aeroload.model;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.JobProfile;

import java.util.Arrays;

@Slf4j
public class FileLineKeyOpProducer extends Consumer<Product<String, String[]>> {
    public static final ListPolicy POLICY = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
    public static final String SET_NAME = "audience_targeting_segments";
    public static final String NAMESPACE = "tempcache";
    public static final String BIN_SEGMENT_NAME = "list";
    private final Consumer<Product<Key, Operation[]>> consumer;

    public FileLineKeyOpProducer(JobProfile profile, Consumer<Product<Key, Operation[]>> consumer) {
        super(profile);
        this.consumer = consumer;
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
    protected void consume(Product<String, String[]> product) {
        try {
            consumer.offer(new Product<>(getKey(product.getA()), getOperations(product.getB()),
                    product.getSuccessHandler(), product.getFailureHandler()));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void interruptedError() {

    }
}
