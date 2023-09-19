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

package kafka.log.streamaspect;

import kafka.server.epoch.EpochEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ElasticLeaderEpochCheckpointMeta {
    private final int version;
    private List<EpochEntry> entries;

    public ElasticLeaderEpochCheckpointMeta(int version, List<EpochEntry> entries) {
        this.version = version;
        this.entries = entries;
    }

    public byte[] encode() {
        int totalLength = 4 // version
                + 4 // following entries size
                + 12 * entries.size(); // all entries
        ByteBuffer buffer = ByteBuffer.allocate(totalLength)
                .putInt(version)
                .putInt(entries.size());
        entries.forEach(entry -> buffer.putInt(entry.epoch()).putLong(entry.startOffset()));
        buffer.flip();
        return buffer.array();
    }

    public static ElasticLeaderEpochCheckpointMeta decode(ByteBuffer buffer) {
        int version = buffer.getInt();
        int entryCount = buffer.getInt();
        List<EpochEntry> entryList = new ArrayList<>(entryCount);
        while (buffer.hasRemaining()) {
            entryList.add(new EpochEntry(buffer.getInt(), buffer.getLong()));
        }
        if (entryList.size() != entryCount) {
            throw new RuntimeException("expect entry count:" + entryCount + ", decoded " + entryList.size() + " entries");
        }
        return new ElasticLeaderEpochCheckpointMeta(version, entryList);
    }

    public int version() {
        return version;
    }

    public List<EpochEntry> entries() {
        return entries;
    }

    public void setEntries(List<EpochEntry> entries) {
        this.entries = entries;
    }
}
