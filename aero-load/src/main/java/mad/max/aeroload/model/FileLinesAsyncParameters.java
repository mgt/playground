package mad.max.aeroload.model;

import java.io.File;

public record FileLinesAsyncParameters(File file, long start, long limit, long errorThreshold, String binName) {
}
