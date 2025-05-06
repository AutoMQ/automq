package kafka.automq.table.worker;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public class ReferenceHolder<T> extends AbstractReferenceCounted {
    private final T value;
    private final Runnable deallocate;

    public ReferenceHolder(T value, Runnable deallocate) {
        this.value = value;
        this.deallocate = deallocate;
    }

    public T value() {
        return value;
    }

    @Override
    protected void deallocate() {
        deallocate.run();
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }
}
