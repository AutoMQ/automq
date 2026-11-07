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

package com.automq.stream.s3;

import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import com.automq.stream.s3.operator.Writer;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * CompositeObject is a logic object which soft links multiple objects together.
 * <p>
 * v0 format:
 * objects
 * object_count u32
 * objects (
 * object_id u64
 * block_start_index u32
 * bucket_index u16
 * )*
 * indexes
 * index_count u32
 * (
 * stream_id u64
 * start_offset u64
 * end_offset_delta u32
 * record_count u32
 * block_start_position u64
 * block_size u32
 * )*
 * index_handle
 * position u64
 * length u32
 * padding 40byte - 8 - 8 - 4
 * magic u64
 */
public class CompositeObject {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeObject.class);
    public static final byte OBJECTS_BLOCK_MAGIC = 0x52;
    public static final int OBJECT_BLOCK_HEADER_SIZE = 1 /* magic */ + 4 /* objects count */;
    public static final int OBJECT_UNIT_SIZE = 8 /* objectId */ + 4 /* blockStartIndex */ + 2 /* bucketId */;

    public static final int FOOTER_SIZE = 48;
    public static final long FOOTER_MAGIC = 0x88e241b785f4cff8L;

    public static CompositeObjectReader reader(S3ObjectMetadata objectMetadata, ObjectReader.RangeReader rangeReader) {
        return new CompositeObjectReader(objectMetadata, rangeReader);
    }

    public static CompositeObjectReader reader(S3ObjectMetadata objectMetadata, ObjectStorage objectStorage) {
        return new CompositeObjectReader(
            objectMetadata,
            (readOptions, metadata, startOffset, endOffset) ->
                objectStorage.rangeRead(
                    new ObjectStorage.ReadOptions().bucket(metadata.bucket()).throttleStrategy(readOptions.throttleStrategy()),
                    metadata.key(), startOffset, endOffset)
        );
    }

    public static CompositeObjectWriter writer(Writer writer) {
        return new CompositeObjectWriter(writer);
    }

    protected static CompletableFuture<Void> deleteWithCompositeObjectReader(
        S3ObjectMetadata objectMetadata,
        ObjectStorage objectStorage,
        CompositeObjectReader reader) {
        return reader.basicObjectInfo().thenCompose(info -> {
            // 2. delete linked object
            List<CompositeObjectReader.ObjectIndex> objectIndexes = ((CompositeObjectReader.BasicObjectInfoExt) info).objectsBlock().indexes();
            List<ObjectPath> objectPaths = objectIndexes
                .stream()
                .map(o -> new ObjectPath(o.bucketId(), ObjectUtils.genKey(0, o.objectId())))
                .collect(Collectors.toList());
            return objectStorage.delete(objectPaths)
                .thenApply(rst -> objectIndexes.stream().map(o -> o.bucketId() + "/" + o.objectId()).collect(Collectors.toList()));
        }).thenCompose(linkedObjects -> {
            // 3. delete composite object
            return objectStorage.delete(List.of(new ObjectPath(objectMetadata.bucket(), objectMetadata.key()))).thenAccept(rst ->
                LOGGER.info("Delete composite object {}/{} success, linked objects: {}",
                    ObjectAttributes.from(objectMetadata.attributes()).bucket(), objectMetadata.objectId(), linkedObjects)
            );
        }).thenAccept(rst -> {
        }).exceptionally(ex -> {
            Throwable cause = FutureUtil.cause(ex);
            if (cause instanceof ObjectNotExistException) {
                // The composite object is already deleted.
                return null;
            }
            throw new CompletionException(cause);
        });
    }

    public static CompletableFuture<Void> delete(S3ObjectMetadata objectMetadata, ObjectStorage objectStorage) {
        @SuppressWarnings("resource")
        CompositeObjectReader reader = reader(objectMetadata, objectStorage);
        return deleteWithCompositeObjectReader(objectMetadata, objectStorage, reader)
            .whenComplete((rst, ex) -> {
                reader.release();
                if (ex != null) {
                    LOGGER.error("Delete composite object {} fail", objectMetadata, ex);
                }
            });
    }
}
