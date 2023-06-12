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

package kafka.log.es;

import com.automq.elasticstream.client.DefaultRecordBatch;
import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.RecordBatchWithContext;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class RecordBatchWithContextWrapper implements RecordBatchWithContext {
    private final RecordBatch recordBatch;
    private final long baseOffset;

    public RecordBatchWithContextWrapper(RecordBatch recordBatch, long baseOffset) {
        this.recordBatch = recordBatch;
        this.baseOffset = baseOffset;
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public long lastOffset() {
        return baseOffset + recordBatch.count();
    }

    @Override
    public int count() {
        return recordBatch.count();
    }

    @Override
    public long baseTimestamp() {
        return recordBatch.baseTimestamp();
    }

    @Override
    public Map<String, String> properties() {
        return recordBatch.properties();
    }

    @Override
    public ByteBuffer rawPayload() {
        return recordBatch.rawPayload().duplicate();
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + recordBatch.rawPayload().remaining())
            .putLong(baseOffset)
            .putInt(recordBatch.count())
            .put(recordBatch.rawPayload().duplicate())
            .flip();
        return buffer.array();
    }

    public static RecordBatchWithContextWrapper decode(ByteBuffer buffer) {
        long baseOffset = buffer.getLong();
        int count = buffer.getInt();
        return new RecordBatchWithContextWrapper(new DefaultRecordBatch(count, 0, Collections.emptyMap(), buffer), baseOffset);
    }
}
