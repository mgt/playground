package mad.max.aeroload.model.transformer.base;

public record InputStreamMeta(String fileName, long start, long limit, long errorThreshold, String binName) {
}
