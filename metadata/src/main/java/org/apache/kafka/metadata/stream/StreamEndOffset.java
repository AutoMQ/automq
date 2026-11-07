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

package org.apache.kafka.metadata.stream;

import java.util.Objects;

public class StreamEndOffset {
    private final long streamId;
    private final long endOffset;

    public StreamEndOffset(long streamId, long endOffset) {
        this.streamId = streamId;
        this.endOffset = endOffset;
    }

    public long streamId() {
        return streamId;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StreamEndOffset that = (StreamEndOffset) o;
        return streamId == that.streamId && endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, endOffset);
    }
}
