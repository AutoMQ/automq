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

package kafka.log.stream.s3;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ZstdUtils {

    public static ByteBuf compress(ByteBuf source, ByteBufAllocator alloc) {
        int uncompressedLength = source.readableBytes();
        ByteBuf directSource = alloc.directBuffer(uncompressedLength);
        source.readBytes(directSource);
        int destLength = uncompressedLength + 1024;
        for (; ; ) {
            ByteBuf dest = alloc.directBuffer(destLength, destLength);
            try {
                int compressedLength = Zstd.compress(dest.nioBuffer(0, destLength), directSource.nioBuffer());
                dest.writerIndex(compressedLength);
                directSource.release();
                return dest;
            } catch (ZstdException e) {
                destLength += 16 * 1024;
                dest.release();
            }
        }
    }
}
