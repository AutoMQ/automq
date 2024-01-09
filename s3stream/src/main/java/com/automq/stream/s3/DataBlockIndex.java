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

package com.automq.stream.s3;

import io.netty.buffer.ByteBuf;

public record DataBlockIndex(long streamId, long startOffset, int endOffsetDelta, int recordCount, long startPosition,
                             int size) {

    public static final int BLOCK_INDEX_SIZE = 8/* streamId */ + 8 /* startOffset */ + 4 /* endOffset delta */
        + 4 /* record count */ + 8 /* block position */ + 4 /* block size */;

    public long endOffset() {
        return startOffset + endOffsetDelta;
    }

    public long endPosition() {
        return startPosition + size;
    }

    public void encode(ByteBuf buf) {
        buf.writeLong(streamId);
        buf.writeLong(startOffset);
        buf.writeInt(endOffsetDelta);
        buf.writeInt(recordCount);
        buf.writeLong(startPosition);
        buf.writeInt(size);
    }
}
