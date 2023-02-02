package mad.max.aeroload.model.producer;

import com.amazonaws.util.Md5Utils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mad.max.aeroload.model.base.Triad;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.base.AsyncProducer;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@Slf4j
public class LocalDirInputStreamProducer extends AsyncProducer<Triad<String, String, InputStream>> implements Runnable{

    private final String localDir;
    public LocalDirInputStreamProducer(String localDir, AsyncConsumer<Triad<String, String, InputStream>> consumer) {
        super(consumer);
        this.localDir = localDir;
    }

    @SneakyThrows
    public void run(){
        try (Stream<Path> walk = Files.walk(Path.of(localDir))) {
            walk.filter(Files::isRegularFile)
                    .map(this::getTriad)
                    .forEach(t -> this.push(t, new AsyncConsumer.Observer() {
                        @SneakyThrows
                        @Override
                        public void onSuccess() {
                            t.c().close();
                        }

                        @Override
                        @SneakyThrows
                        public void onFail(String error) {
                            t.c().close();
                        }
                    }));
        }
    }

    @SneakyThrows
    private Triad<String, String, InputStream> getTriad(Path path) {
        FileInputStream data = new FileInputStream(path.toFile());
        return new Triad<>(path.getFileName().toString(), Md5Utils.md5AsBase64(DigestUtils.md5(data)), data);
    }
}

