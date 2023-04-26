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

import sdk.elastic.stream.api.KeyValue;
import sdk.elastic.stream.api.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class RecordBatchWrapper implements RecordBatch {
    private org.apache.kafka.common.record.RecordBatch inner;

    public RecordBatchWrapper(
            org.apache.kafka.common.record.RecordBatch inner) {
        this.inner = inner;
    }

    @Override
    public int count() {
        return inner.countOrNull();
    }

    @Override
    public long baseTimestamp() {
        // TODO: fix record batch
        return inner.maxTimestamp();
    }

    @Override
    public List<KeyValue> properties() {
        return Collections.emptyList();
    }

    @Override
    public ByteBuffer rawPayload() {
        // TODO: expose kafka under Bytebuf slice
        ByteBuffer buf = ByteBuffer.allocate(inner.sizeInBytes());
        inner.writeTo(buf);
        buf.flip();
        return buf;
    }
}
