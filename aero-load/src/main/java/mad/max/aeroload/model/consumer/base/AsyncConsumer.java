package mad.max.aeroload.model.consumer.base;

import java.util.function.Consumer;

public interface AsyncConsumer<T> extends Consumer<T> {

    void accept(T product, Observer observer);

    default void accept(T product) {
        this.accept(product, VOID_OBSERVER);
    }

    interface Observer {
        void onSuccess();
        void onFail(String error);
    }

    Observer VOID_OBSERVER = new Observer() {
        @Override
        public void onSuccess() {

        }

        @Override
        public void onFail(String error) {

        }
    };
}
