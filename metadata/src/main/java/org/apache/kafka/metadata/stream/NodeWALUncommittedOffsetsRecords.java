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
package org.apache.kafka.metadata.stream;

import org.apache.kafka.common.metadata.NodeWALUncommittedOffsetsRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Builds bounded metadata records for node-scoped historical WAL responsibility changes.
 */
public final class NodeWALUncommittedOffsetsRecords {
    public static final int MAX_ENTRIES_PER_RECORD = 10_000;

    private NodeWALUncommittedOffsetsRecords() {
    }

    /**
     * Split entries into record-version-zero chunks suitable for one atomic Controller result.
     */
    public static List<ApiMessageAndVersion> create(
        int nodeId, Collection<NodeWALUncommittedOffset> offsets
    ) {
        List<NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset> entries = offsets.stream()
            .map(offset -> new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
                .setStreamId(offset.streamId())
                .setStartOffset(offset.startOffset())
                .setEndOffset(offset.endOffset()))
            .toList();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (int from = 0; from < entries.size(); from += MAX_ENTRIES_PER_RECORD) {
            int to = Math.min(from + MAX_ENTRIES_PER_RECORD, entries.size());
            records.add(new ApiMessageAndVersion(new NodeWALUncommittedOffsetsRecord()
                .setNodeId(nodeId).setEntries(new ArrayList<>(entries.subList(from, to))), (short) 0));
        }
        return records;
    }
}
