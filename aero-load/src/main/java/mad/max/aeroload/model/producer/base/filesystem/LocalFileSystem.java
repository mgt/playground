package mad.max.aeroload.model.producer.base.filesystem;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Objects;
import java.util.stream.Stream;

import static org.springframework.util.StringUtils.hasText;

@Slf4j
public class LocalFileSystem implements FileSystem {

    private final String localDir;

    public LocalFileSystem(String localDir) {
        this.localDir = localDir;
    }


    @SneakyThrows
    @Override
    public Iterable<String> ls() {
        return Stream.of(Objects.requireNonNull(Path.of(localDir).toFile().list()))
                .map(f-> Path.of(localDir, f))
                .map(Path::toString)
                .toList();
    }


    @SneakyThrows
    public Pair<InputStreamMeta, InputStream> get(String filePath) {
        File file = Path.of(filePath).toFile();
        FileInputStream data = new FileInputStream(file);
        InputStreamMeta inputStreamMeta =
                new InputStreamMeta(filePath, file.length(), "", new Date(file.lastModified()));
        return new Pair<>(inputStreamMeta, data);
    }


    @Override
    public boolean fileExist(String fileName) {
        return new File(fileName).exists();
    }


    @SneakyThrows
    public void createFile(String fileName, String content) {
        Path p = Files.createFile(Path.of(fileName));
        if (hasText(content))
            FileUtils.write(p.toFile(), content, StandardCharsets.UTF_8);
    }
}

