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
package org.apache.kafka.tiered.storage.specs;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

public final class RemoteFetchSpec {

    private final int sourceBrokerId;
    private final TopicPartition topicPartition;
    private final int count;

    /**
     * Specifies a fetch (download) event from a second-tier storage. This is used to ensure the
     * interactions between Kafka and the second-tier storage match expectations.
     *
     * @param sourceBrokerId The broker which fetched (a) remote log segment(s) from the second-tier storage.
     * @param topicPartition The topic-partition which segment(s) were fetched.
     * @param count The number of remote log segment(s) fetched.
     */
    public RemoteFetchSpec(int sourceBrokerId,
                           TopicPartition topicPartition,
                           int count) {
        this.sourceBrokerId = sourceBrokerId;
        this.topicPartition = topicPartition;
        this.count = count;
    }

    public int getSourceBrokerId() {
        return sourceBrokerId;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return String.format("RemoteFetch[source-broker-id=%d topic-partition=%s count=%d]",
                sourceBrokerId, topicPartition, count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteFetchSpec that = (RemoteFetchSpec) o;
        return sourceBrokerId == that.sourceBrokerId
                && count == that.count
                && Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceBrokerId, topicPartition, count);
    }
}
