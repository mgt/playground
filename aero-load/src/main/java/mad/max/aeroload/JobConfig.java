package mad.max.aeroload;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class JobConfig {
    public static final int CAPACITY = 1000;
    public static final int THREAD_SLEEP_MAX = 1000;
    public static final int THREAD_SLEEP_MIN = 100;
    @Value("${file.path}")
    private String filePath;
    @Value("${file.header:false}")
    boolean hasHeader;
    @Value("${file.delimiter}")
    String delimiter;
    @Value("${file.segmentDelimiter}")
    String segmentDelimiter;
    @Value("${file.segmentIndexInFile:1}")
    int segmentIndexInFile;
    @Value("${aerospike.maxThroughput:10}")
    private int maxThroughput;
}
