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

package kafka.log.streamaspect.cache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileCacheTest {

    @Test
    public void test() throws IOException {
        FileCache fileCache = new FileCache("/tmp/file_cache_test", 10 * 1024, 1024);

        // occupy block 0,1
        fileCache.put("test1", 10, genBuf((byte) 1, 2 * 1024));

        ByteBuf rst = fileCache.get("test1", 10 + 1000, 1024).get();
        assertEquals(1024, rst.readableBytes());
        assertTrue(verify(rst, (byte) 1));

        Assertions.assertFalse(fileCache.get("test1", 10 + 1000, 2048).isPresent());

        // occupy block 2,3
        fileCache.put("test2", 233, genBuf((byte) 2, 1025));

        // occupy block 4~8
        fileCache.put("test2", 2048, genBuf((byte) 4, 1024 * 5));

        // occupy block 9
        fileCache.put("test2", 10000, genBuf((byte) 5, 1024));

        // touch lru
        assertEquals(1025, fileCache.get("test2", 233, 1025).get().readableBytes());
        assertEquals(1024, fileCache.get("test2", 10000, 1024).get().readableBytes());
        assertEquals(2048, fileCache.get("test1", 10, 2048).get().readableBytes());
        assertEquals(1024 * 5, fileCache.get("test2", 2048, 1024 * 5).get().readableBytes());

        // expect evict test2-233 and test2-10000
        fileCache.put("test3", 123, genBuf((byte) 6, 2049));

        FileCache.Value value = fileCache.path2cache.get("test3").get(123L);
        assertEquals(2049, value.dataLength);
        assertArrayEquals(new int[] {2, 3, 9}, value.blocks);


        rst = fileCache.get("test3", 123, 2049).get();
        assertEquals(2049, rst.readableBytes());
        assertTrue(verify(rst, (byte) 6));

        // expect evict test1-10 and test2-2048
        fileCache.put("test4", 123, genBuf((byte) 7, 2049));
        value = fileCache.path2cache.get("test4").get(123L);
        assertArrayEquals(new int[] {0, 1, 4}, value.blocks);
        rst = fileCache.get("test4", 123, 2049).get();
        assertTrue(verify(rst, (byte) 7));

        assertEquals(4, fileCache.freeBlockCount);

        // expect occupy free blocks 5,6,7
        fileCache.put("test5", 123, genBuf((byte) 8, 2049));
        value = fileCache.path2cache.get("test5").get(123L);
        assertArrayEquals(new int[] {5, 6, 7}, value.blocks);
        rst = fileCache.get("test5", 123, 2049).get();
        assertTrue(verify(rst, (byte) 8));
        assertEquals(1, fileCache.freeBlockCount);
    }

    ByteBuf genBuf(byte data, int length) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, data);
        return Unpooled.wrappedBuffer(bytes);
    }

    boolean verify(ByteBuf buf, byte data) {
        buf = buf.duplicate();
        while (buf.readableBytes() > 0) {
            if (buf.readByte() != data) {
                return false;
            }
        }
        return true;
    }

}
