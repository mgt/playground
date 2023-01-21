package mad.max.aeroload.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public final class Product<A, B> {
    private A a;
    private B b;
    private Runnable successHandler;
    private Runnable failureHandler;

}
