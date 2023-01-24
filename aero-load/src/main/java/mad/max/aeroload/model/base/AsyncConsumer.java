package mad.max.aeroload.model.base;

import java.util.function.Consumer;

public interface AsyncConsumer<T> extends Consumer<T> {

    void accept(T product, Observer observer);

    interface Observer {
        void onSuccess();
        void onFail(String error);
    }
}
