package mad.max.aeroload.model.producer.base;

import lombok.Getter;

import java.util.regex.Pattern;

@Getter
public enum LinesReaderConfigs {
    SEGMENTS(new ReadingConfig(Pattern.compile(".*_web_segments_.*|.*_device_segments_.*"), "/upload","\t", ",", 1, false, true)),
    DEFAULT(new ReadingConfig(Pattern.compile(".*"),"","\t", ",", 1, false, false)),
    RETRY(new ReadingConfig(Pattern.compile(".*\\.rety"), "","\t", ",", 2, true, false ));

    public static final String SEGMENT_BIN_NAME = "segments" ;
    private final ReadingConfig readingConfig;

    LinesReaderConfigs(ReadingConfig readingConfig) {
        this.readingConfig = readingConfig;
    }

    public record ReadingConfig(Pattern namePatter, String folderLocation, String delimiter, String segmentDelimiter, int segmentColumnIndexInFile, boolean hasHeader, boolean isCompressed) {
    }
}
