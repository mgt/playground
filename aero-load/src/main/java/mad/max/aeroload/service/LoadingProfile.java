package mad.max.aeroload.service;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class LoadingProfile {

    private int maxThroughput;
    private long maxLinesPerFile;
    private int maxParallelCommands;
    private int maxQueuedElements;
    private int maxErrorThreshold;

    @Getter
    public enum PredefinedProfiles {
        DEFAULT(new LoadingProfile(20, Long.MAX_VALUE, 50, 100, 5)),
        CONSERVATIVE(new LoadingProfile(10, Long.MAX_VALUE, 20, 1000,10)),
        PERFORMANCE(new LoadingProfile(100, Long.MAX_VALUE, 100, 1000, 1));
        private final LoadingProfile profile;

        PredefinedProfiles(LoadingProfile profile) {
            this.profile = profile;
        }
    }

}
