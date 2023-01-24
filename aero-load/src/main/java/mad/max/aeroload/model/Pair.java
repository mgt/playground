package mad.max.aeroload.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public final class Pair<A, B> {
    private A a;
    private B b;
}
