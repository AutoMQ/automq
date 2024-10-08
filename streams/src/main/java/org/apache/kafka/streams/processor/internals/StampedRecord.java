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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

public class StampedRecord extends Stamped<ConsumerRecord<?, ?>> {
    private final ConsumerRecord<byte[], byte[]> rawRecord;

    public StampedRecord(final ConsumerRecord<?, ?> record, final long timestamp, final ConsumerRecord<byte[], byte[]> rawRecord) {
        super(record, timestamp);
        this.rawRecord = rawRecord;
    }

    public String topic() {
        return value.topic();
    }

    public int partition() {
        return value.partition();
    }

    public Object key() {
        return value.key();
    }

    public Object value() {
        return value.value();
    }

    public long offset() {
        return value.offset();
    }

    public Headers headers() {
        return value.headers();
    }

    public ConsumerRecord<byte[], byte[]> rawRecord() {
        return rawRecord;
    }

    @Override
    public boolean equals(final Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return value.toString() + ", timestamp = " + timestamp;
    }
}
