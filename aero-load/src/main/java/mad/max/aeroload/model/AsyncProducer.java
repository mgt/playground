package mad.max.aeroload.model;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class AsyncProducer<T> {
    private AsyncConsumer<T> consumer;

    protected void push(T product, AsyncConsumer.Observer observer){
        consumer.accept(product, observer);
    }
}
