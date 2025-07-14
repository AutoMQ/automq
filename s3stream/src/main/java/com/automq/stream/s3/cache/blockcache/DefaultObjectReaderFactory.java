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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.ObjectReaderLRUCache;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.operator.ObjectStorage;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class DefaultObjectReaderFactory implements ObjectReaderFactory {
    private static final int MAX_OBJECT_READER_SIZE = 100 * 1024 * 1024; // 100MB;

    private final ObjectReaderLRUCache objectReaders;
    private final Supplier<ObjectStorage> objectStorage;

    public DefaultObjectReaderFactory(ObjectStorage objectStorage) {
        this(() -> objectStorage);
    }

    public DefaultObjectReaderFactory(Supplier<ObjectStorage> objectStorage) {
        this.objectReaders = new ObjectReaderLRUCache("ObjectReader", MAX_OBJECT_READER_SIZE);
        this.objectStorage = objectStorage;
    }

    @Override
    public synchronized ObjectReader get(S3ObjectMetadata metadata) {
        AtomicReference<ObjectReader> objectReaderRef = new AtomicReference<>();
        objectReaders.inLockRun(() -> {
            ObjectReader objectReader = objectReaders.computeIfAbsent(metadata.objectId(), k -> ObjectReader.reader(metadata, objectStorage.get()));
            objectReader.retain();
            objectReaderRef.set(objectReader);
        });
        return objectReaderRef.get();
    }

    @Override
    public ObjectStorage getObjectStorage() {
        return objectStorage.get();
    }
}
