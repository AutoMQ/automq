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

public class PartitionMetric implements Element {
    private int partition;
    private long watermark;
    private final Schema avroSchema;

    public static final Schema AVRO_SCHEMA = SchemaBuilder.builder()
        .record(PartitionMetric.class.getName())
        .fields()
        .name("partition").type().intType().noDefault()
        .name("watermark").type().longType().noDefault()
        .endRecord();

    public PartitionMetric(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public PartitionMetric(int partition, long watermark) {
        this.partition = partition;
        this.watermark = watermark;
        this.avroSchema = AVRO_SCHEMA;
    }

    public int partition() {
        return partition;
    }

    public long watermark() {
        return watermark;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.partition = (int) v;
                return;
            case 1:
                this.watermark = (long) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return partition;
            case 1:
                return watermark;
            default:
                throw new UnsupportedOperationException("Unknown field oridinal: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public String toString() {
        return "PartitionMetrics{" +
            "partition=" + partition +
            ", watermark=" + watermark +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        PartitionMetric metric = (PartitionMetric) o;
        return partition == metric.partition && watermark == metric.watermark;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, watermark);
    }
}
