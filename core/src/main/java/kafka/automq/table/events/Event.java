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

package kafka.automq.table.events;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Event implements Element {
    private long timestamp;
    private EventType type;
    private Payload payload;
    private Schema avroSchema;

    // used by avro deserialize reflection
    public Event(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public Event(long timestamp, EventType type, Payload payload) {
        this.timestamp = timestamp;
        this.type = type;
        this.payload = payload;
        avroSchema = SchemaBuilder.builder().record(Event.class.getName())
            .fields()
            .name("timestamp").type().longType().noDefault()
            .name("type").type().intType().noDefault()
            .name("payload").type(payload.getSchema()).noDefault()
            .endRecord();
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.timestamp = (long) v;
                return;
            case 1:
                this.type = EventType.fromId((Integer) v);
                return;
            case 2:
                this.payload = (Payload) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return timestamp;
            case 1:
                return type.id();
            case 2:
                return payload;
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    public long timestamp() {
        return timestamp;
    }

    public EventType type() {
        return type;
    }

    public <T> T payload() {
        //noinspection unchecked
        return (T) payload;
    }
}
