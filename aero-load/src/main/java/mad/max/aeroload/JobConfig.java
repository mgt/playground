package mad.max.aeroload;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class JobConfig {
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


}
