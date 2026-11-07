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

package kafka.automq.table.coordinator;

import kafka.automq.table.events.AvroCodec;
import kafka.automq.table.events.Element;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Checkpoint implements Element {
    private Status status;
    private UUID commitId;
    private Long taskOffset;
    private long[] nextOffsets;
    private UUID lastCommitId;
    private Long lastCommitTimestamp;
    private long[] preCommitOffsets;
    private final Schema avroSchema;

    private static final Schema AVRO_SCHEMA = SchemaBuilder.builder().record(Checkpoint.class.getName())
        .fields()
        .name("status").type().intType().noDefault()
        .name("commitId").type(UUID_SCHEMA).noDefault()
        .name("taskOffset").type().longType().noDefault()
        .name("nextOffsets").type().array().items().longType().noDefault()
        .name("lastCommitId").type(UUID_SCHEMA).noDefault()
        .name("lastCommitTimestamp").type().longType().noDefault()
        .name("preCommitOffsets").type().array().items().longType().arrayDefault(Collections.emptyList())
        .endRecord();

    public Checkpoint(Schema schema) {
        this.avroSchema = schema;
    }

    public Checkpoint(Status status, UUID commitId, Long taskOffset, long[] nextOffsets,
        UUID lastCommitId, Long lastCommitTimestamp, long[] preCommitOffsets) {
        this.status = status;
        this.commitId = commitId;
        this.taskOffset = taskOffset;
        this.nextOffsets = nextOffsets;
        this.lastCommitId = lastCommitId;
        this.lastCommitTimestamp = lastCommitTimestamp;
        this.preCommitOffsets = preCommitOffsets;
        this.avroSchema = AVRO_SCHEMA;
    }

    public Status status() {
        return status;
    }

    public UUID commitId() {
        return commitId;
    }

    public Long taskOffset() {
        return taskOffset;
    }

    public long[] nextOffsets() {
        return nextOffsets;
    }

    public UUID lastCommitId() {
        return lastCommitId;
    }

    public Long lastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public long[] preCommitOffsets() {
        return preCommitOffsets;
    }

    public static Checkpoint decode(ByteBuffer buf) {
        try {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return AvroCodec.decode(bytes);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public byte[] encode() {
        try {
            return AvroCodec.encode(this);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0: {
                this.status = Status.fromCode((Integer) v);
                break;
            }
            case 1: {
                this.commitId = Element.toUuid((GenericData.Fixed) v);
                break;
            }
            case 2: {
                this.taskOffset = (Long) v;
                break;
            }
            case 3: {
                //noinspection unchecked
                this.nextOffsets = ((List<Long>) v).stream().mapToLong(l -> l).toArray();
                break;
            }
            case 4: {
                this.lastCommitId = Element.toUuid((GenericData.Fixed) v);
                break;
            }
            case 5: {
                this.lastCommitTimestamp = (Long) v;
                break;
            }
            case 6: {
                //noinspection unchecked
                this.preCommitOffsets = ((List<Long>) v).stream().mapToLong(l -> l).toArray();
                break;
            }
            default: {
                throw new IndexOutOfBoundsException("Invalid index: " + i);
            }
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0: {
                return status.code();
            }
            case 1: {
                return Element.toFixed(commitId);
            }
            case 2: {
                return taskOffset;
            }
            case 3: {
                return Arrays.stream(nextOffsets).boxed().collect(Collectors.toList());
            }
            case 4: {
                return Element.toFixed(lastCommitId);
            }
            case 5: {
                return lastCommitTimestamp;
            }
            case 6: {
                return Arrays.stream(preCommitOffsets).boxed().collect(Collectors.toList());
            }
            default: {
                throw new IndexOutOfBoundsException("Invalid index: " + i);
            }
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
            "status=" + status +
            ", commitId=" + commitId +
            ", taskOffset=" + taskOffset +
            ", nextOffsets=" + Arrays.toString(nextOffsets) +
            ", lastCommitId=" + lastCommitId +
            ", lastCommitTimestamp=" + lastCommitTimestamp +
            ", preCommitOffsets=" + Arrays.toString(preCommitOffsets) +
            '}';
    }
}
