package mad.max.aeroload.model.producer.base;

import lombok.Getter;

@Getter
public enum LinesReaderConfigs {
    SEGMENTS(new ReadingConfig("\t", ",", 1, false, true)),
    DEFAULT(new ReadingConfig("\t", ",", 1, false, false)),
    RETRY(new ReadingConfig("\t", ",", 2, true, false ));

    public static final String SEGMENT_BIN_NAME = "segments" ;
    private final ReadingConfig readingConfig;

    LinesReaderConfigs(ReadingConfig readingConfig) {
        this.readingConfig = readingConfig;
    }

    public record ReadingConfig(String delimiter, String segmentDelimiter, int segmentColumnIndexInFile, boolean hasHeader, boolean isCompressed) {
    }
}
