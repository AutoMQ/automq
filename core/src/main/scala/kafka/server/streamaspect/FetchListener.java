/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.server.streamaspect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchMetadata;

public interface FetchListener {
    int NONE_SESSION_ID = FetchMetadata.INVALID_SESSION_ID;
    FetchListener NOOP = new FetchListener() {
        @Override
        public void onFetch(TopicPartition topicPartition, int sessionId, long fetchOffset, long timestamp) {
        }

        @Override
        public void onSessionClosed(TopicPartition topicPartition, int sessionId) {
        }
    };

    /**
     * Reports one fetched partition result.
     * <p>
     * For now, {@code fetchOffset} and {@code timestamp} are derived from the last batch in returned records:
     * {@code lastBatch.lastOffset()} and {@code lastBatch.maxTimestamp()}.
     */
    void onFetch(TopicPartition topicPartition, int sessionId, long fetchOffset, long timestamp);

    void onSessionClosed(TopicPartition topicPartition, int sessionId);
}
