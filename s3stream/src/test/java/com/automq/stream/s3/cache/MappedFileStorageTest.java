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

package com.automq.stream.s3.cache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("S3Unit")
@Timeout(60)
public class MappedFileStorageTest {

    /**
     * Copies across several mappings and a shorter final mapping without changing buffer limits.
     */
    @Test
    public void testCrossMappingCopies(@TempDir Path directory) throws IOException {
        Path path = directory.resolve("cache");
        MappedFileStorage storage = new MappedFileStorage(path.toString(), 37, 10);
        byte[] sourceBytes = new byte[40];
        for (int i = 0; i < sourceBytes.length; i++) {
            sourceBytes[i] = (byte) (i + 1);
        }
        ByteBuffer source = ByteBuffer.wrap(sourceBytes);
        source.position(3);
        source.limit(32);
        storage.write(5, source);
        assertEquals(32, source.position());
        assertEquals(32, source.limit());

        ByteBuffer target = ByteBuffer.allocate(40);
        target.position(2);
        target.limit(39);
        storage.read(0, target);
        byte[] expected = new byte[40];
        System.arraycopy(sourceBytes, 3, expected, 7, 29);
        assertArrayEquals(expected, target.array());
        assertEquals(39, target.position());
        assertEquals(39, target.limit());
        assertEquals(37L, Files.size(path));

        ByteBuffer finalBytes = ByteBuffer.allocate(3);
        storage.read(31, finalBytes);
        assertArrayEquals(Arrays.copyOfRange(sourceBytes, 29, 32), finalBytes.array());
    }

    /**
     * A disabled cache creates no backing file and permits empty transfers.
     */
    @Test
    public void testZeroCapacity(@TempDir Path directory) throws IOException {
        Path path = directory.resolve("cache");
        MappedFileStorage storage = new MappedFileStorage(path.toString(), 0);
        storage.write(0, ByteBuffer.allocate(0));
        storage.read(0, ByteBuffer.allocate(0));
        assertFalse(Files.exists(path));
    }
}
