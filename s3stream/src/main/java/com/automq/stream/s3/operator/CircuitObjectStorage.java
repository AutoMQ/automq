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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.CircuitBreaker;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The circuit object storage only adapts to write ahead log uploading and block cache reading.
 */
public class CircuitObjectStorage implements ObjectStorage {
    public static final String PROTOCOL = "circuit";
    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitObjectStorage.class);

    private final ObjectStorage mainStorage;
    private final ObjectStorage fallbackStorage;
    private final CircuitBreaker circuitBreaker;
    private final AtomicInteger halfOpenMainWriterLimiter = new AtomicInteger(1);

    public CircuitObjectStorage(ObjectStorage mainStorage, ObjectStorage fallbackStorage,
        CircuitBreaker circuitBreaker) {
        this.mainStorage = mainStorage;
        this.fallbackStorage = fallbackStorage;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public boolean readinessCheck() {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public synchronized Writer writer(WriteOptions options, String objectPath) {
        if (circuitBreaker == null) {
            throw new UnsupportedOperationException();
        }
        CircuitBreaker.CircuitStatus status = circuitBreaker.status();
        switch (circuitBreaker.status()) {
            case OPEN:
                return new CircuitWriter(options, objectPath, CircuitBreaker.CircuitStatus.OPEN);
            case HALF_OPEN:
                while (true) {
                    int limiter = halfOpenMainWriterLimiter.get();
                    if (limiter == 0) {
                        return new FallbackWriter(fallbackStorage.writer(options, objectPath));
                    } else {
                        if (halfOpenMainWriterLimiter.compareAndSet(limiter, limiter - 1)) {
                            return new CircuitWriter(options, objectPath, CircuitBreaker.CircuitStatus.HALF_OPEN);
                        }
                    }
                }
            case CLOSED:
                return new FallbackWriter(fallbackStorage.writer(options, objectPath));
            default:
                throw new IllegalStateException("Unknown circuit status " + status);
        }
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end) {
        if (options.bucket() == mainStorage.bucketId() || options.bucket() == ObjectAttributes.MATCH_ALL_BUCKET) {
            return mainStorage.rangeRead(options, objectPath, start, end);
        } else if (options.bucket() == fallbackStorage.bucketId()) {
            return fallbackStorage.rangeRead(options, objectPath, start, end);
        } else {
            return FutureUtil.failedFuture(new IllegalArgumentException(String.format("Cannot match bucket id in (%s, %s)",
                mainStorage.bucketId(), fallbackStorage.bucketId())));
        }
    }

    @Override
    public CompletableFuture<List<ObjectInfo>> list(String prefix) {
        List<ObjectInfo> list = new ArrayList<>();
        return CompletableFuture.allOf(
            mainStorage.list(prefix).thenAccept(list::addAll),
            fallbackStorage.list(prefix).thenAccept(list::addAll)
        ).thenApply(nil -> list);
    }

    @Override
    public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
        boolean allMainStorage = true;
        for (ObjectPath objectPath : objectPaths) {
            if (objectPath.bucketId() != mainStorage.bucketId()) {
                allMainStorage = false;
                break;
            }
        }
        if (allMainStorage) {
            return mainStorage.delete(objectPaths);
        } else {
            List<ObjectPath> mainObjectPaths = new ArrayList<>(objectPaths.size());
            List<ObjectPath> fallbackObjectPaths = new ArrayList<>(objectPaths.size());
            for (ObjectPath objectPath : objectPaths) {
                if (objectPath.bucketId() == fallbackStorage.bucketId()) {
                    fallbackObjectPaths.add(objectPath);
                } else {
                    mainObjectPaths.add(objectPath);
                }
            }
            return CompletableFuture.allOf(mainStorage.delete(mainObjectPaths), fallbackStorage.delete(fallbackObjectPaths));
        }
    }

    @Override
    public short bucketId() {
        throw new UnsupportedOperationException();
    }

    public class CircuitWriter implements Writer {
        private final WriteOptions options;
        private final String objectPath;
        private final List<Pair<ByteBuf, CompletableFuture<Void>>> writeList = new LinkedList<>();
        private final CircuitBreaker.CircuitStatus status;
        private Writer writer;

        public CircuitWriter(WriteOptions options, String objectPath, CircuitBreaker.CircuitStatus status) {
            this.options = options;
            this.objectPath = objectPath;
            this.writer = mainStorage.writer(options.copy().timeout(TimeUnit.SECONDS.toMillis(30)), objectPath);
            this.status = status;
        }

        @Override
        public synchronized CompletableFuture<Void> write(ByteBuf data) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            writeList.add(Pair.of(data.retain(), cf));
            writer.write(data);
            return cf.whenComplete((nil, ex) -> data.release());
        }

        @Override
        public void copyOnWrite() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBatchingPart() {
            return false;
        }

        @Override
        public CompletableFuture<Void> close() {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            writer.close().whenComplete((rst, ex) -> {
                if (ex == null) {
                    writeList.forEach(p -> p.getValue().complete(null));
                    cf.complete(null);
                    if (status == CircuitBreaker.CircuitStatus.HALF_OPEN) {
                        halfOpenMainWriterLimiter.getAndUpdate(old -> old + (old == 0 ? 2 : 1));
                    }
                    return;
                }
                if (status == CircuitBreaker.CircuitStatus.HALF_OPEN) {
                    halfOpenMainWriterLimiter.getAndUpdate(old -> Math.max(old - 1, 1));
                }
                circuitBreaker.addFallbackTask(cf);
                circuitBreaker.transitionTo(CircuitBreaker.CircuitStatus.CLOSED).whenComplete((nil, ex2) -> {
                    writer = fallbackStorage.writer(options.copy(), objectPath);
                    writeList.forEach(p -> FutureUtil.propagate(writer.write(p.getKey()), p.getValue()));
                    FutureUtil.propagate(writer.close(), cf);
                });
            });
            return cf;
        }

        @Override
        public CompletableFuture<Void> release() {
            return writer.release().thenAccept(ignore -> writeList.forEach(p -> p.getValue().complete(null)));
        }

        @Override
        public short bucketId() {
            return writer.bucketId();
        }
    }

    public class FallbackWriter implements Writer {
        private final Writer writer;

        public FallbackWriter(Writer writer) {
            this.writer = writer;
        }

        @Override
        public CompletableFuture<Void> write(ByteBuf data) {
            return writer.write(data);
        }

        @Override
        public void copyOnWrite() {
            writer.copyOnWrite();
        }

        @Override
        public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            writer.copyWrite(s3ObjectMetadata, start, end);
        }

        @Override
        public boolean hasBatchingPart() {
            return writer.hasBatchingPart();
        }

        @Override
        public CompletableFuture<Void> close() {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            circuitBreaker.addFallbackTask(cf);
            FutureUtil.propagate(writer.close(), cf);
            return cf;
        }

        @Override
        public CompletableFuture<Void> release() {
            return writer.release();
        }

        @Override
        public short bucketId() {
            return writer.bucketId();
        }
    }

}
