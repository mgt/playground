package mad.max.aeroload.model.producer.base;

import lombok.Getter;

import java.util.function.Function;
import java.util.regex.Pattern;

@Getter
public enum LoadingFileType {
    SEGMENTS(new ReadingConfig(Pattern.compile(".*_web_segments_.*|.*_device_segments_.*"), "/upload","\t", ",", 1, false, true),
    fn->"segments"
    ),
    DEFAULT(new ReadingConfig(Pattern.compile(".*"),"","\t", ",", 1, false, false) , Function.identity()),
    RETRY(new ReadingConfig(Pattern.compile(".*\\.rety"), "","\t", ",", 2, true, false ), Function.identity());

    private final ReadingConfig readingConfig;
    private final Function<String, String > namingFunction;

    LoadingFileType(ReadingConfig readingConfig, Function<String,String> namingFunction) {
        this.readingConfig = readingConfig;
        this.namingFunction = namingFunction;
    }

    public record ReadingConfig(Pattern namePatter, String folderLocation, String delimiter, String segmentDelimiter, int segmentColumnIndexInFile, boolean hasHeader, boolean isCompressed) {
    }
}
