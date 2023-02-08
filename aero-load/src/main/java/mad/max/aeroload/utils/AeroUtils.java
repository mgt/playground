package mad.max.aeroload.utils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import lombok.experimental.UtilityClass;

@UtilityClass
public class AeroUtils {
     public static final String SET_NAME = "audience_targeting_segments";
     public static final String NAMESPACE = "tempcache";
     public static final String SEGMENT_BIN_NAME = "segments" ;
     private static final ListPolicy POLICY = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);

     public static Key getKey(String key){
         return new Key(NAMESPACE, SET_NAME, key );
     }

     public static AerospikeClient aerospikeClient(String h, int p, int to, int ttl, int maxParallelCommands ) {
         EventPolicy eventPolicy = new EventPolicy();

         //limit maxCommandsInProcess on each event loop.
         // This is the thread-safe way to limit inflight commands (and thus connections).
         eventPolicy.maxCommandsInProcess = maxParallelCommands;

         //The async command queue is used for commands when maxCommandsInProcess limit is reached.
         // If the rate of incoming commands consistently exceeds the rate at which commands are processed,
         // then your application can potentially run out of memory.
         // To avoid this case, you may need to limit your queue size.
         eventPolicy.maxCommandsInQueue = maxParallelCommands;

         EventLoops eventLoops = new NioEventLoops(eventPolicy, Runtime.getRuntime().availableProcessors());

         ClientPolicy policy = new ClientPolicy();
         policy.eventLoops = eventLoops;
         policy.maxConnsPerNode = eventPolicy.maxCommandsInProcess * eventLoops.getSize();

         WritePolicy writePolicy = new WritePolicy();
         writePolicy.setTimeout(to);
         writePolicy.sendKey = true;
         writePolicy.expiration = ttl * 86400;
         writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
         policy.writePolicyDefault = writePolicy;

         Host[] hosts = Host.parseHosts(h, p);

         return new AerospikeClient(policy, hosts);
     }

     //This method gets invoked alot, so imperative paradig. for performance
     public static Operation[] getAppendIfNotExistOperations(String binName, String... segmentsArray) {
         Operation[] ops = new Operation[segmentsArray.length];
         for (int i = 0; i < segmentsArray.length; i++) {
             ops[i] = ListOperation.append(POLICY, binName, Value.get(segmentsArray[i]));
         }
         return ops;
     }
}
