package mad.max.aeroload.model;

import lombok.AllArgsConstructor;

import java.util.function.Consumer;

@AllArgsConstructor
public abstract class Producer<T> {
    private Consumer<T> consumer;

    protected void push(T product){
        consumer.accept(product);
    }
}
