package mad.max.aeroload.model.producer;

import lombok.Getter;

@Getter
public enum LinesReaderConfigs {
    DEFAULT(new ReadingConfig("\t", ",", 1, false)),
    RETRY(new ReadingConfig("\t", ",", 2, true));

    private final ReadingConfig readingConfig;

    LinesReaderConfigs(ReadingConfig readingConfig) {
        this.readingConfig = readingConfig;
    }

    public record ReadingConfig(String delimiter, String segmentDelimiter, int segmentColumnIndexInFile, boolean hasHeader) {
    }
}