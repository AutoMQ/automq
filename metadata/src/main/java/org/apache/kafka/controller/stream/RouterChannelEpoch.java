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
import java.util.Objects;

import io.netty.buffer.ByteBuf;

public class RouterChannelEpoch {
    public static final String ROUTER_CHANNEL_EPOCH_KEY = "__a_r_c/metadata";
    private static final Schema SCHEMA0 = SchemaBuilder.record("RouterChannelEpoch").fields()
        .name("committed").type().longType().noDefault()
        .name("fenced").type().longType().noDefault()
        .name("current").type().longType().noDefault()
        .name("lastBumpUpTimestamp").type().longType().noDefault()
        .endRecord();
    private long committed;
    private long fenced;
    private long current;
    private long lastBumpUpTimestamp;

    public RouterChannelEpoch(long committed, long fenced, long current, long lastBumpUpTimestamp) {
        this.committed = committed;
        this.fenced = fenced;
        this.current = current;
        this.lastBumpUpTimestamp = lastBumpUpTimestamp;
    }

    public long getCommitted() {
        return committed;
    }

    public void setCommitted(long committed) {
        this.committed = committed;
    }

    public long getFenced() {
        return fenced;
    }

    public void setFenced(long fenced) {
        this.fenced = fenced;
    }

    public long getCurrent() {
        return current;
    }

    public void setCurrent(long current) {
        this.current = current;
    }

    public long getLastBumpUpTimestamp() {
        return lastBumpUpTimestamp;
    }

    public void setLastBumpUpTimestamp(long lastBumpUpTimestamp) {
        this.lastBumpUpTimestamp = lastBumpUpTimestamp;
    }

    @Override
    public String toString() {
        return "RouterChannelEpoch{" +
            "committed=" + committed +
            ", fenced=" + fenced +
            ", current=" + current +
            ", lastBumpUpTimestamp=" + lastBumpUpTimestamp +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        RouterChannelEpoch that = (RouterChannelEpoch) o;
        return committed == that.committed && fenced == that.fenced && current == that.current && lastBumpUpTimestamp == that.lastBumpUpTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(committed, fenced, current, lastBumpUpTimestamp);
    }

    public static ByteBuf encode(RouterChannelEpoch routerChannelEpoch, short version) {
        if (version != 0) {
            throw new IllegalArgumentException("version must be 0");
        }
        GenericRecord record = new GenericData.Record(SCHEMA0);
        record.put("committed", routerChannelEpoch.committed);
        record.put("fenced", routerChannelEpoch.fenced);
        record.put("current", routerChannelEpoch.current);
        record.put("lastBumpUpTimestamp", routerChannelEpoch.lastBumpUpTimestamp);

        try {
            return AvroUtils.encode(record, (short) 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static RouterChannelEpoch decode(ByteBuf buf) {
        try {
            GenericRecord record = AvroUtils.decode(buf, version -> {
                if (version != 0) {
                    throw new IllegalStateException("unsupported version: " + version);
                }
                return SCHEMA0;
            });
            return new RouterChannelEpoch(
                (Long) record.get("committed"),
                (Long) record.get("fenced"),
                (Long) record.get("current"),
                (Long) record.get("lastBumpUpTimestamp")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
