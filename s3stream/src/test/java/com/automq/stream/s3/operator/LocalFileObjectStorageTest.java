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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("S3Unit")
public class LocalFileObjectStorageTest {

    LocalFileObjectStorage objectStorage;
    String base = "/tmp/automq_test/localfilestoragetest";

    @BeforeEach
    public void setup() {
        objectStorage = new LocalFileObjectStorage(BucketURI.parse("-2@file://" + base));
    }

    @AfterEach
    public void cleanup() throws IOException {
        Utils.delete(Path.of(base));
    }

    @Test
    public void testReadWrite() throws ExecutionException, InterruptedException {
        String key = ObjectUtils.genKey(0, 100);
        Writer writer = objectStorage.writer(new ObjectStorage.WriteOptions(), key);
        writer.write(Unpooled.wrappedBuffer("hello ".getBytes(StandardCharsets.UTF_8)));
        byte[] bytes = "world".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = ByteBufAlloc.byteBuffer(bytes.length);
        buf.writeBytes(bytes);
        writer.write(buf);
        writer.close().get();
        assertEquals(0, buf.refCnt());

        buf = objectStorage.rangeRead(new ObjectStorage.ReadOptions(), key, 0, -1L).get();
        assertEquals("hello world", substr(buf, 0, buf.readableBytes()));
        assertEquals("hello", substr(buf, 0, 5));

        objectStorage.delete(List.of(new ObjectStorage.ObjectInfo(objectStorage.bucketId(), key, 0, 0))).get();

        Throwable exception = null;
        try {
            objectStorage.rangeRead(new ObjectStorage.ReadOptions(), key, 0, -1L).get();
        } catch (Throwable e) {
            exception = FutureUtil.cause(e);
        }
        assertEquals(ObjectNotExistException.class, Optional.ofNullable(exception).map(Throwable::getClass).orElse(null));
    }

    @Test
    public void testList() throws ExecutionException, InterruptedException {
        objectStorage.write(new ObjectStorage.WriteOptions(), "abc/def/100", Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8))).get();
        objectStorage.write(new ObjectStorage.WriteOptions(), "abc/def/101", Unpooled.wrappedBuffer("hello world1".getBytes(StandardCharsets.UTF_8))).get();
        objectStorage.write(new ObjectStorage.WriteOptions(), "abc/deg/102", Unpooled.wrappedBuffer("hello world2".getBytes(StandardCharsets.UTF_8))).get();

        assertEquals(
            List.of("abc/def/100", "abc/def/101", "abc/deg/102"),
            objectStorage.list("").get().stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList())
        );
        assertEquals(
            List.of("abc/def/100", "abc/def/101"),
            objectStorage.list("abc/def").get().stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList())
        );
        assertEquals(
            List.of("abc/def/100", "abc/def/101", "abc/deg/102"),
            objectStorage.list("abc/de").get().stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList())
        );
        assertEquals(
            List.of("abc/def/100", "abc/def/101", "abc/deg/102"),
            objectStorage.list("ab").get().stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList())
        );
        assertEquals(
            List.of("abc/def/100", "abc/def/101"),
            objectStorage.list("abc/def/").get().stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList())
        );
        assertEquals(
            List.of("abc/def/100", "abc/def/101"),
            objectStorage.list("abc/def/1").get().stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList())
        );
        assertEquals(
            Collections.emptyList(),
            objectStorage.list("abc/deh").get().stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList())
        );
    }

    @Test
    public void testDiskFull() throws Throwable {
        objectStorage.availableSpace.set(10);
        String key = ObjectUtils.genKey(0, 100);
        objectStorage.write(new ObjectStorage.WriteOptions(), "abc/def/100", Unpooled.wrappedBuffer("hhhhhhhhh".getBytes(StandardCharsets.UTF_8))).get();
        CompletableFuture<?> w2 = objectStorage.write(new ObjectStorage.WriteOptions(), "abc/def/101", Unpooled.wrappedBuffer("h2".getBytes(StandardCharsets.UTF_8)));
        CompletableFuture<?> w3 = objectStorage.write(new ObjectStorage.WriteOptions(), "abc/def/102", Unpooled.wrappedBuffer("h3".getBytes(StandardCharsets.UTF_8)));
        assertEquals(2, objectStorage.waitingTasks.size());
        assertEquals(1, objectStorage.availableSpace.get());
        assertFalse(w2.isDone());
        assertFalse(w3.isDone());
        objectStorage.delete(List.of(new ObjectStorage.ObjectInfo(objectStorage.bucketId(), "abc/def/100", 0, 0))).get();
        w2.get(1, TimeUnit.SECONDS);
        w3.get(1, TimeUnit.SECONDS);
        assertEquals(0, objectStorage.waitingTasks.size());
        assertEquals(6, objectStorage.availableSpace.get());
    }

    private String substr(ByteBuf buf, int start, int end) {
        buf = buf.duplicate();
        byte[] bytes = new byte[end - start];
        buf.skipBytes(start);
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
