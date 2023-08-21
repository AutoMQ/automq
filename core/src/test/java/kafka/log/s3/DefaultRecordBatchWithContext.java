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

package kafka.log.s3;

import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.RecordBatchWithContext;

import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultRecordBatchWithContext implements RecordBatchWithContext {
    private final RecordBatch recordBatch;
    private final long baseOffset;

    public DefaultRecordBatchWithContext(RecordBatch recordBatch, long baseOffset) {
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
        return recordBatch.rawPayload();
    }
}
