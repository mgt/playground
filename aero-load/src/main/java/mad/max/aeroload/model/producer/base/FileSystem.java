package mad.max.aeroload.model.producer.base;

import mad.max.aeroload.model.base.Pair;

import java.io.InputStream;

public interface FileSystem {
    Iterable<String> ls();

    Pair<InputStreamMeta, InputStream> get(String key);

    boolean fileExist(String fileName);

    void createFile(String fileName, String content);
}
