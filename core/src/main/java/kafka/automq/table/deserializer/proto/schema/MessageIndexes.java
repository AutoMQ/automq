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

package kafka.automq.table.deserializer.proto.schema;

import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MessageIndexes {
    private static final List<Integer> DEFAULT_INDEX = Collections.singletonList(0);
    private final List<Integer> indexes;

    public MessageIndexes(List<Integer> indexes) {
        this.indexes = new ArrayList<>(indexes);
    }

    public List<Integer> getIndexes() {
        return Collections.unmodifiableList(indexes);
    }

    public byte[] toBytes() {
        if (indexes.size() == 1 && indexes.get(0) == 0) {
            // optimization
            ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(0));
            ByteUtils.writeVarint(0, buffer);
            return buffer.array();
        }
        int size = ByteUtils.sizeOfVarint(indexes.size());
        for (Integer index : indexes) {
            size += ByteUtils.sizeOfVarint(index);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteUtils.writeVarint(indexes.size(), buffer);
        for (Integer index : indexes) {
            ByteUtils.writeVarint(index, buffer);
        }
        return buffer.array();
    }

    public List<Integer> indexes() {
        return indexes;
    }

    private int calculateSize() {
        // 4 bytes for size + 4 bytes per index
        return 4 + (indexes.size() * 4);
    }

    public static MessageIndexes readFrom(ByteBuffer buffer) {
        int size = ByteUtils.readVarint(buffer);
        if (size == 0) {
            return new MessageIndexes(DEFAULT_INDEX);
        }
        List<Integer> indexes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            indexes.add(ByteUtils.readVarint(buffer));
        }
        return new MessageIndexes(indexes);
    }
}
