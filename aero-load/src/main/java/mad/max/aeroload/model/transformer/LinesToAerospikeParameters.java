package mad.max.aeroload.model.transformer;

import java.util.function.Function;

public record LinesToAerospikeParameters(String namespace, String setName, Function<String, String> segmentNamingFunction, String lineDelimiter, int segmentColumnIndexInFile, String segmentDelimiter ) {
}
