/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.image;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;

public class RegistryRef extends AbstractReferenceCounted {
    public static final RegistryRef NOOP = new RegistryRef(new SnapshotRegistry(new LogContext()), -1, List.of());

    private final SnapshotRegistry registry;
    private final long epoch;
    private final List<Long> liveEpochs;

    public RegistryRef() {
        this(new SnapshotRegistry(new LogContext()), -1, new ArrayList<>());
    }

    public RegistryRef(SnapshotRegistry registry, long epoch, List<Long> liveEpochs) {
        this.registry = registry;
        this.epoch = epoch;
        this.liveEpochs = liveEpochs;
    }

    public SnapshotRegistry registry() {
        return registry;
    }

    public long epoch() {
        return epoch;
    }

    public RegistryRef next() {
        return inLock(() -> {
            long newEpoch = epoch + 1;
            liveEpochs.add(newEpoch);
            registry.getOrCreateSnapshot(newEpoch);
            return new RegistryRef(registry, newEpoch, liveEpochs);
        });
    }

    @Override
    public ReferenceCounted retain(int increment) {
        if (this == NOOP) {
            throw new UnsupportedOperationException("retain is not supported for NOOP");
        }
        return super.retain(increment);
    }

    @Override
    public ReferenceCounted retain() {
        if (this == NOOP) {
            throw new UnsupportedOperationException("retain is not supported for NOOP");
        }
        return super.retain();
    }

    @Override
    public boolean release() {
        if (this == NOOP) {
            throw new UnsupportedOperationException("release is not supported for NOOP");
        }
        return super.release();
    }

    @Override
    public boolean release(int decrement) {
        if (this == NOOP) {
            throw new UnsupportedOperationException("release is not supported for NOOP");
        }
        return super.release(decrement);
    }

    @Override
    protected void deallocate() {
        inLock(() -> {
            if (liveEpochs.isEmpty()) {
                throw new IllegalStateException("liveEpochs is empty");
            }
            long oldFirst = liveEpochs.get(0);
            liveEpochs.remove(epoch);
            if (liveEpochs.isEmpty()) {
                throw new IllegalStateException("liveEpochs is empty");
            }
            long newFirst = liveEpochs.get(0);
            if (newFirst != oldFirst) {
                registry.deleteSnapshotsUpTo(newFirst);
            }
        });
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }

    public void inLock(Runnable task) {
        if (registry == null) {
            task.run();
            return;
        }
        synchronized (registry) {
            task.run();
        }
    }

    public <T> T inLock(Supplier<T> task) {
        if (registry == null) {
            return task.get();
        }
        synchronized (registry) {
            return task.get();
        }
    }
}
