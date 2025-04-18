/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.image;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

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
