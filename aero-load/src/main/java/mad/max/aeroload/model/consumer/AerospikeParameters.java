package mad.max.aeroload.model.consumer;

import java.util.function.Function;

public record AerospikeParameters(int maxErrorThreshold, int maxThroughput, int maxParallelCommands, int maxQueuedElements) {
}
