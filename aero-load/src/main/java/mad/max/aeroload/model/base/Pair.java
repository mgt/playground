package mad.max.aeroload.model.base;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public final class Pair<A, B> {
    private A a;
    private B b;
}
