package mad.max.aeroload.model;

import lombok.Getter;

@Getter
public enum FileReadingConfigs {
    DEFAULT(new ReadingConfig("\t", ",", 1, false)),
    RETRY(new ReadingConfig("\t", ",", 2, true));

    private final ReadingConfig readingConfig;

    FileReadingConfigs(ReadingConfig readingConfig) {
        this.readingConfig = readingConfig;
    }

    public record ReadingConfig(String delimiter, String segmentDelimiter, int segmentColumnIndexInFile, boolean hasHeader) {
    }
}
