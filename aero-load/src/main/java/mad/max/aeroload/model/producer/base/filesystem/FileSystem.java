package mad.max.aeroload.model.producer.base.filesystem;

import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.producer.base.InputStreamMeta;

import java.io.InputStream;

public interface FileSystem {
    Iterable<String> ls();

    Pair<InputStreamMeta, InputStream> get(String key);

    boolean fileExist(String fileName);

    void createFile(String fileName, String content);
}
