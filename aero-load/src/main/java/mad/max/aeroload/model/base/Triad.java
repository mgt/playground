package mad.max.aeroload.model.base;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public final class Triad<A, B, C> {
    private A a;
    private B b;
    private C c;
}
