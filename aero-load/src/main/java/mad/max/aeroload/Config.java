package mad.max.aeroload;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static mad.max.aeroload.JobConfig.CAPACITY;

@Configuration
public class Config {
    @Value("${aerospike.host:localhost}")
    private String host;
    @Value("${aerospike.port:3000}")
    private int port;
    @Value("${aerospike.timeout:}")
    private int timeout;
    @Value("${aerospike.ttl-days:1}")
    private Integer ttl;



    @Bean(destroyMethod = "close")
    public AerospikeClient aerospikeClient() {
        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.maxCommandsInProcess = CAPACITY;
        eventPolicy.maxCommandsInQueue = CAPACITY;

        EventLoops eventLoops = new NioEventLoops(eventPolicy, Runtime.getRuntime().availableProcessors());

        ClientPolicy policy = new ClientPolicy();
        policy.eventLoops = eventLoops;

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.setTimeout(timeout);
        writePolicy.sendKey = true;
        writePolicy.expiration = ttl*86400;
        writePolicy.recordExistsAction= RecordExistsAction.UPDATE;
        policy.writePolicyDefault = writePolicy;

        Host[] hosts = Host.parseHosts(host, port);

        return new AerospikeClient(policy, hosts);
    }

    @Bean
    Throttles throttles() {
        return new Throttles(Runtime.getRuntime().availableProcessors(), CAPACITY);
    }


}
