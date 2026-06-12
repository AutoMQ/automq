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

package org.apache.kafka.controller.availability;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AvailabilityTarget {
    public enum Kind {
        CLUSTER,
        BROKER,
        TOPIC_PARTITION,
        TOPIC_PARTITION_OFFSET
    }

    private Kind kind;
    private int brokerId;
    private long brokerEpoch;
    private String topic;
    private int partition;
    private long offset;

    public AvailabilityTarget() {
    }

    private AvailabilityTarget(Kind kind, int brokerId, long brokerEpoch, String topic, int partition, long offset) {
        this.kind = kind;
        this.brokerId = brokerId;
        this.brokerEpoch = brokerEpoch;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public static AvailabilityTarget cluster() {
        return new AvailabilityTarget(Kind.CLUSTER, -1, -1, null, -1, -1);
    }

    public static AvailabilityTarget broker(int brokerId, long brokerEpoch) {
        return new AvailabilityTarget(Kind.BROKER, brokerId, brokerEpoch, null, -1, -1);
    }

    public static AvailabilityTarget topicPartition(String topic, int partition) {
        return new AvailabilityTarget(Kind.TOPIC_PARTITION, -1, -1, topic, partition, -1);
    }

    public static AvailabilityTarget topicPartitionOffset(String topic, int partition, long offset) {
        return new AvailabilityTarget(Kind.TOPIC_PARTITION_OFFSET, -1, -1, topic, partition, offset);
    }

    public Kind getKind() {
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public long getBrokerEpoch() {
        return brokerEpoch;
    }

    public void setBrokerEpoch(long brokerEpoch) {
        this.brokerEpoch = brokerEpoch;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AvailabilityTarget)) {
            return false;
        }
        AvailabilityTarget that = (AvailabilityTarget) o;
        return brokerId == that.brokerId &&
            brokerEpoch == that.brokerEpoch &&
            partition == that.partition &&
            offset == that.offset &&
            kind == that.kind &&
            Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, brokerId, brokerEpoch, topic, partition, offset);
    }

    @Override
    public String toString() {
        return "AvailabilityTarget{" +
            "kind=" + kind +
            ", brokerId=" + brokerId +
            ", brokerEpoch=" + brokerEpoch +
            ", topic='" + topic + '\'' +
            ", partition=" + partition +
            ", offset=" + offset +
            '}';
    }
}
