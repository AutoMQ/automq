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
package org.apache.kafka.controller.stream;

import org.apache.kafka.controller.automq.utils.AvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class NodeCommittedEpoch {
    public static final String NODE_COMMITED_EPOCH_KEY_PREFIX = "__a_r_c_nce/";
    private static final Schema SCHEMA0 = SchemaBuilder.record("NodeCommitedEpoch").fields()
        .name("epoch").type().longType().noDefault()
        .endRecord();
    private long epoch;

    public NodeCommittedEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String toString() {
        return "NodeCommitedEpoch{" +
            "epoch=" + epoch +
            '}';
    }

    public static ByteBuf encode(NodeCommittedEpoch nodeCommittedEpoch, int version) {
        if (version != 0) {
            throw new IllegalArgumentException("version must be 0");
        }
        GenericRecord record = new GenericData.Record(SCHEMA0);
        record.put("epoch", nodeCommittedEpoch.epoch);

        try {
            return AvroUtils.encode(record, (short) 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static NodeCommittedEpoch decode(ByteBuf buf) {
        try {
            GenericRecord record = AvroUtils.decode(buf, version -> {
                if (version != 0) {
                    throw new IllegalStateException("unsupported version: " + version);
                }
                return SCHEMA0;
            });
            return new NodeCommittedEpoch((Long) record.get("epoch"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
