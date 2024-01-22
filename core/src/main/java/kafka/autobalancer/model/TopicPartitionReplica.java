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

package kafka.autobalancer.model;

import kafka.autobalancer.common.RawMetricType;
import kafka.autobalancer.common.Resource;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TopicPartitionReplica extends RawMetricInstance {
    private final TopicPartition tp;

    public TopicPartitionReplica(TopicPartition tp) {
        this.tp = tp;
    }

    public TopicPartitionReplica(TopicPartitionReplica other) {
        this.tp = new TopicPartition(other.tp.topic(), other.tp.partition());
        clone(other);
    }

    public Set<Resource> getResources() {
        return this.resources;
    }

    public void deriveLoads() {
        for (Map.Entry<RawMetricType, Double> entry : metricsMap.entrySet()) {
            if (entry.getKey().metricScope() != RawMetricType.MetricScope.PARTITION) {
                continue;
            }
            switch (entry.getKey()) {
                case TOPIC_PARTITION_BYTES_IN:
                    this.setLoad(Resource.NW_IN, entry.getValue());
                    break;
                case TOPIC_PARTITION_BYTES_OUT:
                    this.setLoad(Resource.NW_OUT, entry.getValue());
                    break;
                default:
                    break;
            }
        }
    }

    public TopicPartition getTopicPartition() {
        return tp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartitionReplica replica = (TopicPartitionReplica) o;
        return tp.equals(replica.tp);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tp);
    }

    public String shortString() {
        return "TopicPartitionReplica{" +
                "tp=" + tp +
                ", " + timeString() +
                ", " + loadString() +
                "}";
    }

    @Override
    public String toString() {
        return "{TopicPartition=" + tp +
                ", " + super.toString() +
                "}";
    }
}
