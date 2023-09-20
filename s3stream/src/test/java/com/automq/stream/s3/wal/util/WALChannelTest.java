/*
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

package com.automq.stream.s3.wal.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

@Tag("S3Unit")
class WALChannelTest {
    WALChannel walChannel;

    @BeforeEach
    void setUp() {
        walChannel = WALChannel.builder(String.format("%s/WALChannelUnitTest.data", System.getenv("HOME")), 1024 * 1024 * 20).build();
        try {
            walChannel.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        walChannel.close();
    }

    ByteBuffer createRandomTextByteBuffer(int size) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);

        for (int i = 0; i < size; i++) {
            byteBuffer.put("ABCDEFGH".getBytes()[i % 8]);
        }

        return byteBuffer.flip();
    }

    @Test
    void testWriteAndRead() throws IOException {
        ByteBuffer byteBuffer = createRandomTextByteBuffer(1024 * 3);
        for (int i = 0; i < 100; i++) {
            try {
                walChannel.write(byteBuffer, (long) i * byteBuffer.limit());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        final String content = "Hello World";
        walChannel.write(ByteBuffer.wrap(content.getBytes()), 100);

        ByteBuffer readBuffer = ByteBuffer.allocate(content.length());
        int read = walChannel.read(readBuffer, 100);

        String readString = new String(readBuffer.array());
        System.out.println(new String(readBuffer.array()));
        System.out.println(read);

        assert read == content.length();
        assert readString.equals(content);
    }
}
