/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream;

import com.automq.stream.s3.ByteBufAlloc;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class ByteBufSeqAllocV2 {
    public static final int HUGE_BUF_SIZE = ByteBufAlloc.getChunkSize().orElse(4 << 20);
    private final static int THRESHOLD_SIZE = 4096;
    private BlockingQueue<Long> costTimes = new ArrayBlockingQueue<>(1000);
    private BlockingQueue<Integer> noUsage = new ArrayBlockingQueue<>(1000);
    private final int allocType;
    private HugeBufList q000;
    private HugeBufList q050;
    private HugeBufList q075;

    @SuppressWarnings("unchecked")
    public ByteBufSeqAllocV2(int allocType, int concurrency) {
        assert concurrency > 0;
        this.allocType = allocType;

        q075 = new HugeBufList(HUGE_BUF_SIZE, 75, 100, null);
        q050 = new HugeBufList(HUGE_BUF_SIZE, 50, 75, q075);
        if (concurrency < 3) {
            q000 = new HugeBufList(HUGE_BUF_SIZE, 0, 100, q050);
            q075.disable(true);
            q050.disable(true);
        } else {
            q000 = new HugeBufList(HUGE_BUF_SIZE, 0, 50, q050);
        }

        q000.headList = q000;
        q050.headList = q000;
        q075.headList = q000;
        for (int i = 0; i < concurrency; i++) {
            q000.unsafeAdd(new HugeBuf(ByteBufAlloc.byteBuffer(HUGE_BUF_SIZE, allocType)));
        }
    }

    public void releaseForTest() {
        doRelease(q000.hugeBufList);
        doRelease(q050.hugeBufList);
        doRelease(q075.hugeBufList);
    }

    private void doRelease(List<HugeBuf> list) {
        list.forEach(item -> {
            assert item.buf.refCnt() == 1;
            item.buf.release();
        });
    }

    public ByteBuf byteBufferForTest(int capacity) {
        long startTime = System.currentTimeMillis();
        ByteBuf byteBuf = byteBuffer(capacity);
        costTimes.add(System.currentTimeMillis() - startTime);
        return byteBuf;
    }

    public BlockingQueue<Long> getCostTimes() {
        return this.costTimes;
    }

    public BlockingQueue<Integer> getNoUsage() {
        return this.noUsage;
    }

    public ByteBuf byteBuffer(int capacity) {
        if (capacity > HUGE_BUF_SIZE) {
            return ByteBufAlloc.byteBuffer(capacity, allocType);
        }
        InnerByteBuf innerByteBuf = new InnerByteBuf();

        if (capacity >= THRESHOLD_SIZE) {
            allocate(innerByteBuf, capacity);
        } else {
            allocateNormal(innerByteBuf, capacity);
        }
        return innerByteBuf.byteBuf;
    }

    public void allocate(InnerByteBuf innerByteBuf, int capacity) {
        if (!(q050.allocate(innerByteBuf, capacity) || q000.allocate(innerByteBuf, capacity))) {
            if (innerByteBuf.byteBuf == null) {
                if (q075.allocate(innerByteBuf, capacity, true)
                    || q050.allocate(innerByteBuf, capacity, true)
                    || q000.allocate(innerByteBuf, capacity, true)) {
                    assert innerByteBuf.byteBuf != null;
                }
            }
        }
    }

    public void allocateNormal(InnerByteBuf innerByteBuf, int capacity) {
        int lIdx = hash(Thread.currentThread().hashCode()) % 6;
        if (lIdx > 2) {
            q075.allocate(innerByteBuf, capacity, true);
        }
        if (innerByteBuf.byteBuf == null && lIdx > 0) {
            q050.allocate(innerByteBuf, capacity, true);
        }
        if (innerByteBuf.byteBuf == null) {
            q000.allocate(innerByteBuf, capacity, true);
        }
        if (innerByteBuf.byteBuf == null) {
            q075.allocate(innerByteBuf, capacity, true);
        }
        assert innerByteBuf.byteBuf != null;
    }

    private static int hash(int hashCode) {
        return hashCode ^ hashCode >> 16;
    }

    static class InnerByteBuf {
        ByteBuf byteBuf;

        void setByteBuf(ByteBuf byteBuf) {
            assert this.byteBuf == null;
            this.byteBuf = byteBuf;
        }
    }

    static class HugeBuf {
        ByteBuf buf;
        int nextIndex;
        private final ReentrantLock lock;

        HugeBuf(ByteBuf buf) {
            this.buf = buf;
            this.nextIndex = 0;
            lock = new ReentrantLock();
        }

        ByteBuf byteBuffer(int capacity) {
            int start = nextIndex;
            nextIndex += capacity;
            ByteBuf slice = buf.retainedSlice(start, capacity);
            slice.writerIndex(slice.readerIndex());

            return slice;
        }

        boolean satisfies(int capacity) {
            return nextIndex + capacity <= buf.capacity();
        }

        void lock() {
            this.lock.lock();
        }

        void unlock() {
            this.lock.unlock();
        }

        boolean tryLock() {
            return this.lock.tryLock();
        }


        void setBuf(ByteBuf buf) {
            assert this.buf == null;
            this.buf = buf;
        }

        void reset() {
            this.buf.release();
            this.buf = null;
            this.nextIndex = 0;
        }
    }


    private class HugeBufList {
        private final HugeBufList nextList;
        private HugeBufList headList;
        private volatile List<HugeBuf> hugeBufList;
        private final int minFreeThreshold;
        private final int maxFreeThreshold;
        private boolean enable = true;

        public HugeBufList(int chunkSize, int minUsage, int maxUsage, HugeBufList nextList) {
            this.nextList = nextList;
            this.hugeBufList = new LinkedList<>();
            this.maxFreeThreshold = minUsage == 100 ? 0 : (int) (chunkSize * (100.0 - minUsage + 0.99999999) / 100L);
            this.minFreeThreshold = maxUsage == 100 ? 0 : (int) (chunkSize * (100.0 - maxUsage + 0.99999999) / 100L);
        }

        public void disable(boolean enable) {
            this.enable = enable;
        }

        private synchronized HugeBuf selectHugeBuf(int capacity, boolean enableAlloc) {
            if (hugeBufList.size() == 0) {
                return null;
            }
            int size = hugeBufList.size();
            int idx = hash(Thread.currentThread().hashCode()) % size;
            int i = idx;
            do {
                HugeBuf hugeBuf = hugeBufList.get(i % size);
                if (hugeBuf.tryLock()) {
                    try {
                        if (hugeBuf.satisfies(capacity)) {
                            return hugeBuf;
                        }
                    } finally {
                        hugeBuf.unlock();
                    }
                }
                i++;
            } while (i - idx < size);

            if (enableAlloc) {
                return hugeBufList.get(idx);
            }
            return null;
        }

        public boolean allocate(InnerByteBuf buf, int capacity) {
            return this.allocate(buf, capacity, false);
        }

        public boolean allocate(InnerByteBuf buf, int capacity, boolean enableAlloc) {
            if (!enable) {
                return false;
            }
            if (capacity > maxFreeThreshold && !enableAlloc || this.hugeBufList.isEmpty()) {
                return false;
            }
            HugeBuf hugeBuf = selectHugeBuf(capacity, enableAlloc);
            if (hugeBuf == null) {
                return false;
            }
            boolean newAlloc = false;
            hugeBuf.lock();
            try {
                if (!hugeBuf.satisfies(capacity)) {
                    if (!enableAlloc) {
                        return false;
                    }
                    noUsage.add(hugeBuf.buf.capacity() - hugeBuf.nextIndex);
                    hugeBuf.reset();
                    ByteBuf newByteBuf = ByteBufAlloc.byteBuffer(HUGE_BUF_SIZE, allocType);
                    hugeBuf.setBuf(newByteBuf);
                    newAlloc = true;
                }
                buf.setByteBuf(hugeBuf.byteBuffer(capacity));
            } finally {
                hugeBuf.unlock();
            }

            tryTransfer(hugeBuf, newAlloc);

            return true;
        }

        private synchronized boolean remove(HugeBuf hugeBuf) {
            return this.hugeBufList.remove(hugeBuf);
        }

        public void tryTransfer(HugeBuf hugeBuf, boolean newAlloc) {
            if (newAlloc && !this.equals(headList)) {
                if (remove(hugeBuf)) {
                    this.headList.add0(hugeBuf);
                }
                return;
            }
            int remainBytes = tryGetRemainBytes(hugeBuf);

            if (remainBytes < 0) {
                return;
            }
            if (remainBytes < this.minFreeThreshold) {
                if (remove(hugeBuf)) {
                    this.nextList.add(hugeBuf);
                }
            }
        }

        private void add(HugeBuf hugeBuf) {
            int remainBytes = getRemainBytes(hugeBuf);
            if (remainBytes < this.minFreeThreshold) {
                this.nextList.add(hugeBuf);
                return;
            }
            add0(hugeBuf);
        }

        private synchronized void add0(HugeBuf hugeBuf) {
            this.hugeBufList.add(hugeBuf);
        }

        public void unsafeAdd(HugeBuf hugeBuf) {
            this.hugeBufList.add(hugeBuf);
        }


        private int getRemainBytes(HugeBuf hugeBuf) {
            int remainBytes;
            hugeBuf.lock();
            try {
                remainBytes = hugeBuf.buf.capacity() - hugeBuf.nextIndex;
            } finally {
                hugeBuf.unlock();
            }
            return remainBytes;
        }

        private int tryGetRemainBytes(HugeBuf hugeBuf) {
            int remainBytes = -1;
            if (hugeBuf.tryLock()) {
                try {
                    remainBytes = hugeBuf.buf.capacity() - hugeBuf.nextIndex;
                } finally {
                    hugeBuf.unlock();
                }
            }
            return remainBytes;
        }
    }
}
