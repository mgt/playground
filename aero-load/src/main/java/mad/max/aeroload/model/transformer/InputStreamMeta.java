package mad.max.aeroload.model.transformer;

public record InputStreamMeta(String fileName, long start, long limit, long errorThreshold, String binName) {
}
