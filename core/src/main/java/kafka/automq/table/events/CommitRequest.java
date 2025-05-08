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
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.util.List;
import java.util.UUID;

public class CommitRequest implements Payload {
    public static final int NOOP_SPEC_ID = -1;
    private UUID commitId;
    private String topic;
    private List<WorkerOffset> offsets;
    private int specId;
    private final Schema avroSchema;

    private static final Schema AVRO_SCHEMA = SchemaBuilder.builder().record(CommitRequest.class.getName())
        .fields()
        .name("commitId").type(UUID_SCHEMA).noDefault()
        .name("topic").type().stringType().noDefault()
        .name("offsets").type().array().items(WorkerOffset.AVRO_SCHEMA).noDefault()
        .name("specId").type().nullable().intType().intDefault(NOOP_SPEC_ID)
        .endRecord();

    // used by avro deserialize reflection
    public CommitRequest(Schema schema) {
        this.avroSchema = schema;
    }

    public CommitRequest(UUID commitId, String topic, List<WorkerOffset> offsets) {
        this(commitId, topic, NOOP_SPEC_ID, offsets);
    }

    public CommitRequest(UUID commitId, String topic, int specId, List<WorkerOffset> offsets) {
        this.commitId = commitId;
        this.topic = topic;
        this.offsets = offsets;
        this.specId = specId;
        this.avroSchema = AVRO_SCHEMA;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.commitId = Element.toUuid((GenericData.Fixed) v);
                return;
            case 1:
                this.topic = ((Utf8) v).toString();
                return;
            case 2:
                //noinspection unchecked
                this.offsets = (List<WorkerOffset>) v;
                return;
            case 3:
                this.specId = (Integer) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return Element.toFixed(commitId);
            case 1:
                return topic;
            case 2:
                return offsets;
            case 3:
                return specId;
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    public UUID commitId() {
        return commitId;
    }

    public String topic() {
        return topic;
    }

    public List<WorkerOffset> offsets() {
        return offsets;
    }

    public int specId() {
        return specId;
    }

    @Override
    public String toString() {
        return "CommitRequest{" +
            "commitId=" + commitId +
            ", topic='" + topic + '\'' +
            ", offsets=" + offsets +
            ", specId=" + specId +
            '}';
    }
}
