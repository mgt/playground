package mad.max.aeroload.model.producer.base;

import java.util.Date;

public record InputStreamMeta(String fileName, long contentLength, String etag, Date lastModified) {
}
