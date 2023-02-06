package mad.max.aeroload.model.transformer;

import java.util.function.Function;

public record LinesReadingParameters(long start, long limit, long errorThreshold, Function<String,String> function) {
}
