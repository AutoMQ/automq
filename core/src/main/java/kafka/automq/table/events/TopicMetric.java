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

import java.util.Objects;

public class TopicMetric implements Element {
    private long fieldCount;
    private final Schema avroSchema;
    public static final Schema AVRO_SCHEMA = SchemaBuilder.builder().record(TopicMetric.class.getName()).fields()
        .name("fieldCount").type().longType().noDefault()
        .endRecord();
    public static final TopicMetric NOOP = new TopicMetric(0);

    public TopicMetric(Schema avroSchema) {
        this.avroSchema = avroSchema;
        this.fieldCount = 0;
    }

    public TopicMetric(long fieldCount) {
        this.fieldCount = fieldCount;
        this.avroSchema = AVRO_SCHEMA;
    }

    public long fieldCount() {
        return fieldCount;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.fieldCount = (long) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return fieldCount;
            default:
                throw new UnsupportedOperationException("Unknown field oridinal: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        TopicMetric metric = (TopicMetric) o;
        return fieldCount == metric.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldCount);
    }

    @Override
    public String toString() {
        return "TopicMetric{" +
            "fieldCount=" + fieldCount +
            '}';
    }
}
