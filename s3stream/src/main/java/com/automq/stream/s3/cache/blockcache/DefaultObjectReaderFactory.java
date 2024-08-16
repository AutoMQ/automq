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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.ObjectReaderLRUCache;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.operator.ObjectStorage;

public class DefaultObjectReaderFactory implements ObjectReaderFactory {
    private static final int MAX_OBJECT_READER_SIZE = 100 * 1024 * 1024; // 100MB;

    private final ObjectReaderLRUCache objectReaders;
    private final ObjectStorage objectStorage;

    public DefaultObjectReaderFactory(ObjectStorage objectStorage) {
        this.objectReaders = new ObjectReaderLRUCache("ObjectReader", MAX_OBJECT_READER_SIZE);
        this.objectStorage = objectStorage;
    }

    @Override
    public synchronized ObjectReader get(S3ObjectMetadata metadata) {
        ObjectReader objectReader = objectReaders.get(metadata.objectId());
        if (objectReader == null) {
            objectReader = ObjectReader.reader(metadata, objectStorage);
            objectReaders.put(metadata.objectId(), objectReader);
        }
        return objectReader.retain();
    }

    @Override
    public ObjectStorage getObjectStorage() {
        return objectStorage;
    }
}
