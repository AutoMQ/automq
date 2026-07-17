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

package com.automq.stream;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.ByteBufSupplier;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Threads;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

/**
 * Allocates sequential retained slices from recyclable fixed-size slabs.
 *
 * <p>The allocator owns one reference to every slab until the slab expires or this allocator is closed. A slab is
 * reused only after all slices derived from it have been released. The heap/direct allocation mode is captured on the
 * first allocation and remains fixed for the allocator lifetime.</p>
 *
 * <p>This class is thread-safe. {@link #close()} prevents new allocations and releases all slab owner references, but
 * does not invalidate slices still retained by callers.</p>
 */
public class RecyclingByteBufSeqAlloc implements ByteBufSupplier, AutoCloseable {
    public static final int DIRECT_SLAB_SIZE = ByteBufAlloc.getChunkSize().orElse(4 << 20);

    private static final int HEAP_SLAB_SIZE = (4 << 20) - 64;
    private static final long HEAP_BYTES_PER_SLOT = 2L << 30;
    private static final int DEFAULT_CONCURRENCY = (int) Math.min(Systems.CPU_CORES,
        Math.max(Systems.HEAP_MEMORY_SIZE / HEAP_BYTES_PER_SLOT, 1));
    private static final long DEFAULT_FREE_TTL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final int MAX_ALLOC_PATH_EVICTIONS = 16;
    private static final String METRIC_SOURCE = "seq_alloc";
    private static final ConcurrentMap<Integer, LongAdder> RETAINED_MEMORY = new ConcurrentHashMap<>();

    private final int allocType;
    private final Slot[] slots;
    private final Object initLock = new Object();
    private final ReentrantLock poolLock = new ReentrantLock();
    private final ArrayDeque<Slab> retired = new ArrayDeque<>();
    private final ArrayDeque<Slab> free = new ArrayDeque<>();
    private final long freeTtlNanos;
    private final ScheduledExecutorService scheduler;
    private final LongSupplier nanoTime;

    private volatile boolean closed;
    private volatile Mode mode;
    private ScheduledFuture<?> maintenanceTask;

    /**
     * Creates an allocator whose concurrency scales with the available processors and maximum heap size.
     *
     * @param allocType allocation type used for direct buffers and metrics
     */
    public RecyclingByteBufSeqAlloc(int allocType) {
        this(allocType, DEFAULT_CONCURRENCY);
    }

    /**
     * Creates an allocator with the requested number of concurrent allocation slots.
     *
     * @param allocType allocation type used for direct buffers and metrics
     * @param concurrency number of independent sequential allocation slots
     */
    public RecyclingByteBufSeqAlloc(int allocType, int concurrency) {
        this(allocType, concurrency, DEFAULT_FREE_TTL_NANOS, Threads.COMMON_SCHEDULER, System::nanoTime);
    }

    RecyclingByteBufSeqAlloc(int allocType, int concurrency, long freeTtlNanos,
        ScheduledExecutorService scheduler, LongSupplier nanoTime) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be positive");
        }
        if (freeTtlNanos <= 0) {
            throw new IllegalArgumentException("freeTtlNanos must be positive");
        }
        this.allocType = allocType;
        this.slots = new Slot[concurrency];
        for (int i = 0; i < concurrency; i++) {
            slots[i] = new Slot();
        }
        this.freeTtlNanos = freeTtlNanos;
        this.scheduler = Objects.requireNonNull(scheduler);
        this.nanoTime = Objects.requireNonNull(nanoTime);
    }

    /**
     * Allocates a buffer with zero reader and writer indexes and exactly the requested capacity.
     *
     * @param capacity requested buffer capacity
     * @return a retained slab slice, or a standalone buffer when the request is larger than a slab
     * @throws IllegalStateException if this allocator has been closed
     */
    @Override
    public ByteBuf alloc(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity must not be negative");
        }
        Mode mode = mode();
        Slot slot = slots[Math.floorMod(Thread.currentThread().hashCode(), slots.length)];
        List<Slab> evicted = null;
        ByteBuf result;
        slot.lock.lock();
        try {
            ensureOpen();
            if (capacity > mode.slabCapacity) {
                return allocateBacking(mode.direct, capacity);
            }
            if (slot.current == null || !slot.current.satisfies(capacity)) {
                Slab slab;
                poolLock.lock();
                try {
                    if (slot.current != null) {
                        retired.offerLast(slot.current);
                        slot.current = null;
                    }
                    long now = nanoTime.getAsLong();
                    harvestFirstReadyRun(now);
                    slab = free.pollLast();
                    evicted = evictExpired(now, MAX_ALLOC_PATH_EVICTIONS);
                } finally {
                    poolLock.unlock();
                }
                if (slab == null) {
                    slab = newSlab(mode);
                }
                slot.current = slab;
            }
            result = slot.current.allocate(capacity);
        } finally {
            slot.lock.unlock();
            releaseOwners(evicted);
        }
        return result;
    }

    /**
     * Stops maintenance and releases every owner reference held by this allocator.
     * Caller-owned retained slices remain valid until callers release them.
     */
    @Override
    public void close() {
        synchronized (initLock) {
            if (closed) {
                return;
            }
            closed = true;
            if (maintenanceTask != null) {
                maintenanceTask.cancel(false);
            }
        }

        List<Slab> toRelease = new ArrayList<>();
        drainSlabs(0, toRelease);
        releaseOwners(toRelease);
    }

    private void drainSlabs(int slotIndex, List<Slab> toRelease) {
        if (slotIndex < slots.length) {
            Slot slot = slots[slotIndex];
            slot.lock.lock();
            try {
                drainSlabs(slotIndex + 1, toRelease);
            } finally {
                slot.lock.unlock();
            }
            return;
        }

        poolLock.lock();
        try {
            for (Slot slot : slots) {
                if (slot.current != null) {
                    toRelease.add(slot.current);
                    slot.current = null;
                }
            }
            toRelease.addAll(retired);
            retired.clear();
            toRelease.addAll(free);
            free.clear();
        } finally {
            poolLock.unlock();
        }
    }

    private Mode mode() {
        Mode current = mode;
        if (current != null) {
            return current;
        }
        synchronized (initLock) {
            ensureOpen();
            if (mode == null) {
                boolean direct = ByteBufAlloc.getPolicy().isDirect();
                int slabCapacity = direct ? DIRECT_SLAB_SIZE : HEAP_SLAB_SIZE;
                LongAdder retainedBytes = retainedMemory(allocType);
                Mode initialized = new Mode(direct, slabCapacity, retainedBytes);
                ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(this::maintain, freeTtlNanos,
                    freeTtlNanos, TimeUnit.NANOSECONDS);
                mode = initialized;
                maintenanceTask = task;
            }
            return mode;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("allocator is closed");
        }
    }

    private Slab newSlab(Mode mode) {
        Slab slab = new Slab(allocateBacking(mode.direct, mode.slabCapacity));
        mode.retainedBytes.add(slab.owner.capacity());
        return slab;
    }

    private ByteBuf allocateBacking(boolean direct, int capacity) {
        if (direct) {
            return ByteBufAlloc.byteBuffer(capacity, allocType);
        }
        return Unpooled.wrappedBuffer(new byte[capacity]).clear();
    }

    private void harvestFirstReadyRun(long now) {
        Slab head = retired.peekFirst();
        if (head != null && head.reusable()) {
            do {
                moveToFree(retired.pollFirst(), now);
                head = retired.peekFirst();
            } while (head != null && head.reusable());
            return;
        }

        boolean found = false;
        Iterator<Slab> iterator = retired.iterator();
        while (iterator.hasNext()) {
            Slab slab = iterator.next();
            if (!slab.reusable()) {
                if (found) {
                    break;
                }
                continue;
            }
            found = true;
            iterator.remove();
            moveToFree(slab, now);
        }
    }

    private void harvestAllReady(long now) {
        Iterator<Slab> iterator = retired.iterator();
        while (iterator.hasNext()) {
            Slab slab = iterator.next();
            if (slab.reusable()) {
                iterator.remove();
                moveToFree(slab, now);
            }
        }
    }

    private void moveToFree(Slab slab, long now) {
        slab.reset();
        slab.freeSinceNanos = now;
        free.offerLast(slab);
    }

    private List<Slab> evictExpired(long now, int limit) {
        Slab slab = free.peekFirst();
        if (slab == null || now - slab.freeSinceNanos < freeTtlNanos) {
            return null;
        }
        List<Slab> evicted = new ArrayList<>(Math.min(limit, MAX_ALLOC_PATH_EVICTIONS));
        do {
            evicted.add(free.pollFirst());
            slab = free.peekFirst();
        } while (evicted.size() < limit && slab != null && now - slab.freeSinceNanos >= freeTtlNanos);
        return evicted;
    }

    private void maintain() {
        List<Slab> evicted;
        poolLock.lock();
        try {
            if (closed) {
                return;
            }
            long now = nanoTime.getAsLong();
            harvestAllReady(now);
            evicted = evictExpired(now, Integer.MAX_VALUE);
        } finally {
            poolLock.unlock();
        }
        releaseOwners(evicted);
    }

    private void releaseOwners(List<Slab> slabs) {
        if (slabs == null) {
            return;
        }
        Mode current = mode;
        for (Slab slab : slabs) {
            current.retainedBytes.add(-slab.owner.capacity());
            ReferenceCountUtil.safeRelease(slab.owner);
        }
    }

    private static LongAdder retainedMemory(int allocType) {
        return RETAINED_MEMORY.computeIfAbsent(allocType, ignored -> {
            LongAdder value = new LongAdder();
            ByteBufAlloc.registerAllocatedMemoryGauge(allocType, METRIC_SOURCE, value::sum);
            return value;
        });
    }

    private static final class Slot {
        private final ReentrantLock lock = new ReentrantLock();
        private Slab current;
    }

    private static final class Mode {
        private final boolean direct;
        private final int slabCapacity;
        private final LongAdder retainedBytes;

        private Mode(boolean direct, int slabCapacity, LongAdder retainedBytes) {
            this.direct = direct;
            this.slabCapacity = slabCapacity;
            this.retainedBytes = retainedBytes;
        }
    }

    private static final class Slab {
        private final ByteBuf owner;
        private int nextIndex;
        private long freeSinceNanos;

        private Slab(ByteBuf owner) {
            this.owner = owner;
        }

        private ByteBuf allocate(int capacity) {
            int start = nextIndex;
            nextIndex += capacity;
            ByteBuf slice = owner.retainedSlice(start, capacity);
            slice.writerIndex(0);
            return slice;
        }

        private boolean satisfies(int capacity) {
            return nextIndex + capacity <= owner.capacity();
        }

        private boolean reusable() {
            return owner.refCnt() == 1;
        }

        private void reset() {
            nextIndex = 0;
        }
    }

}
