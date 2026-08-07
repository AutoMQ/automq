/*
 * Copyright 2026, AutoMQ HK Limited.
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
package kafka.log.streamaspect.reassignment;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AutomqPreparePartitionHandoffRequestData;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An immutable, frozen MetaStream handoff identified only by topic, partition, and handoff end offset.
 */
public final class PartitionHandoff {
    private static final short API_VERSION = 0;

    private final Key key;
    private final MetaStreamHandoff metaStreamHandoff;
    private final int encodedSize;

    /**
     * Creates a handoff from the identity supplied by partition metadata and a frozen MetaStream snapshot.
     *
     * @param topicId topic id
     * @param partitionId partition id
     * @param metaStreamHandoff frozen MetaStream snapshot and end offset
     */
    public PartitionHandoff(Uuid topicId, int partitionId, MetaStreamHandoff metaStreamHandoff) {
        this.metaStreamHandoff = Objects.requireNonNull(metaStreamHandoff, "metaStreamHandoff");
        this.key = new Key(topicId, partitionId, this.metaStreamHandoff.endOffset());
        this.encodedSize = new AutomqPreparePartitionHandoffRequestData()
            .setHandoffs(List.of(toProtocol()))
            .size(new ObjectSerializationCache(), API_VERSION);
    }

    /**
     * Reconstructs an immutable handoff received through the prepare RPC.
     *
     * @param data encoded protocol handoff
     * @return immutable handoff
     */
    public static PartitionHandoff fromProtocol(AutomqPreparePartitionHandoffRequestData.Handoff data) {
        List<MetaStreamHandoffRecord> records = data.records().stream()
            .map(record -> new MetaStreamHandoffRecord(
                record.baseOffset(), ByteBuffer.wrap(record.metaKeyValue())))
            .collect(Collectors.toList());
        return new PartitionHandoff(data.topicId(), data.partitionIndex(),
            new MetaStreamHandoff(data.metaStreamHandoffEndOffset(), records));
    }

    /**
     * Encodes this handoff for the prepare RPC without adding validation fields outside the handoff identity.
     *
     * @return protocol handoff
     */
    public AutomqPreparePartitionHandoffRequestData.Handoff toProtocol() {
        return new AutomqPreparePartitionHandoffRequestData.Handoff()
            .setTopicId(topicId())
            .setPartitionIndex(partitionId())
            .setMetaStreamHandoffEndOffset(endOffset())
            .setRecords(metaStreamHandoff.records().stream()
                .map(record -> new AutomqPreparePartitionHandoffRequestData.HandoffRecord()
                    .setBaseOffset(record.baseOffset())
                    .setMetaKeyValue(toByteArray(record.encodedMetaKeyValue())))
                .collect(Collectors.toList()));
    }

    /**
     * Returns the exact cache identity.
     */
    public Key key() {
        return key;
    }

    /**
     * Returns the topic id.
     */
    public Uuid topicId() {
        return key.topicId();
    }

    /**
     * Returns the partition id.
     */
    public int partitionId() {
        return key.partitionId();
    }

    /**
     * Returns the frozen MetaStream handoff end offset.
     */
    public long endOffset() {
        return key.metaStreamHandoffEndOffset();
    }

    /**
     * Returns the immutable frozen MetaStream snapshot.
     */
    public MetaStreamHandoff metaStreamHandoff() {
        return metaStreamHandoff;
    }

    /**
     * Returns the actual encoded request-body size of a request containing only this handoff.
     */
    public int encodedSize() {
        return encodedSize;
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    /**
     * Exact handoff cache identity; it deliberately excludes ownership and stream validation fields.
     */
    public record Key(Uuid topicId, int partitionId, long metaStreamHandoffEndOffset) {
        /**
         * Creates an exact cache key from the only fields that define handoff identity.
         */
        public Key {
            Objects.requireNonNull(topicId, "topicId");
        }
    }
}
