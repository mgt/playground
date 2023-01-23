package mad.max.aeroload;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class JobProfile {

    private int maxThroughput;
    private long maxLinesPerFile;
    private int maxParallelCommands;
    private int maxQueuedElements;


    @Getter
    public enum PredefinedProfiles {
        DEFAULT(new JobProfile(20, Long.MAX_VALUE, 50, 100)),
        CONSERVATIVE(new JobProfile(10, Long.MAX_VALUE, 20, 1000)),
        PERFORMANCE(new JobProfile(50, Long.MAX_VALUE, 100, 1000));
        private final JobProfile profile;

        PredefinedProfiles(JobProfile profile) {
            this.profile = profile;
        }
    }

}
