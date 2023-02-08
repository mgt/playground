package mad.max.aeroload.model.transformer;

public record LinesReadingParameters(long start, long limit, long errorThreshold, boolean hasHeader, boolean compressed) {
}
