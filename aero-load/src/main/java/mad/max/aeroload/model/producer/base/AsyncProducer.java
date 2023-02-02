package mad.max.aeroload.model.producer.base;

import lombok.AllArgsConstructor;
import mad.max.aeroload.model.consumer.base.AsyncConsumer;

@AllArgsConstructor
public abstract class AsyncProducer<T> {
    private AsyncConsumer<T> consumer;

    protected void push(T product, AsyncConsumer.Observer observer){
        consumer.accept(product, observer);
    }

}
