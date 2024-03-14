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

import com.automq.stream.api.RecordBatch;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

public class DefaultRecordBatch implements RecordBatch {
    int count;
    ByteBuffer payload;

    public static RecordBatch of(int count, int size) {
        DefaultRecordBatch record = new DefaultRecordBatch();
        record.count = count;
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        record.payload = ByteBuffer.wrap(bytes);
        return record;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long baseTimestamp() {
        return 0;
    }

    @Override
    public Map<String, String> properties() {
        return Collections.emptyMap();
    }

    @Override
    public ByteBuffer rawPayload() {
        return payload.duplicate();
    }
}
