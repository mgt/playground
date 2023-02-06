package mad.max.aeroload.model.producer;

import mad.max.aeroload.model.base.Pair;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;
import mad.max.aeroload.model.producer.base.AsyncProducer;
import mad.max.aeroload.model.producer.base.FileSystem;
import mad.max.aeroload.model.producer.base.InputStreamMeta;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;

public class InputStreamProducer extends AsyncProducer<Pair<InputStreamMeta, InputStream>> implements Runnable {

    private final List<Predicate<InputStreamMeta>> filters = new ArrayList<>();
    private final FileSystem fileSystem;

    public InputStreamProducer(AsyncConsumer<Pair<InputStreamMeta, InputStream>> consumer, FileSystem fileSystem) {
        super(consumer);
        this.fileSystem = fileSystem;
        addFileNotExistFilter(im -> successName(im.fileName()));
        addFileNotExistFilter(im -> failureName(im.fileName()));
    }

    public void addFilter(Predicate<InputStreamMeta> filter) {
        filters.add(filter);
    }

    public void run() {
        for (String fileName : fileSystem.ls()) {

            if (!fileName.contains("_part000"))
                continue;

            CompletableFuture<Void> future = this.processMultipart(fileName, 0,CompletableFuture.completedFuture(null) );
            future.join();
        }
    }

    private void process(String key) {

        Pair<InputStreamMeta, InputStream> product = fileSystem.get(key);

        if (this.shouldProcess(product.a())) {
            this.push(product, new AsyncConsumer.Observer() {
                @Override
                public void onSuccess() {

                    closeInputStream();
                    fileSystem.createFile(successName(product.a().fileName()), "");
                }

                @Override
                public void onFail(String error) {

                    closeInputStream();
                    fileSystem.createFile(failureName(product.a().fileName()), error);
                }

                private void closeInputStream() {
                    try {
                        product.b().close();
                    } catch (IOException e) {
                        // We are guesses in this party, shouldn't throw exception (?)
                        // throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    private static String failureName(String name) {
        return FilenameUtils.getFullPath(name) + FilenameUtils.getBaseName(name) + ".failure";
    }

    private static String successName(String name) {
        return FilenameUtils.getFullPath(name)+ FilenameUtils.getBaseName(name) + ".success";
    }

    private static String partName(String key, int i) {
        if (i == 0)
            return key;
        String part = "_part%03d".formatted(i);
        return FilenameUtils.getBaseName(key).replace("_part000", part) + part + FilenameUtils.getExtension(key);
    }

    private CompletableFuture<Void> processMultipart(String key, int i, CompletableFuture<Void> previous) {
        String newKey = partName(key, i);
        if (fileSystem.fileExist(newKey)) {
            CompletableFuture<Void> c = CompletableFuture.allOf(previous, CompletableFuture.runAsync(()->this.process(newKey)));
            return processMultipart(key, i+1, c);
        }
        return previous;
    }

    private boolean shouldProcess(InputStreamMeta meta) {
        return filters.stream().allMatch(f -> f.test(meta));
    }

    private void addFileNotExistFilter(Function<InputStreamMeta, String> f) {
        this.filters.add(im -> !fileSystem.fileExist(f.apply(im)));
    }
}
