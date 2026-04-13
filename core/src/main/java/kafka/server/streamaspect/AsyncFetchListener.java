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

import kafka.server.CachedPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class AsyncFetchListener implements FetchListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFetchListener.class);

    private final FetchListener delegate;
    private final ExecutorService executor;

    public AsyncFetchListener(FetchListener delegate, ExecutorService executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    @Override
    public void onFetch(TopicPartition topicPartition, int sessionId, long fetchOffset, long timestamp) {
        try {
            executor.execute(() -> {
                try {
                    delegate.onFetch(topicPartition, sessionId, fetchOffset, timestamp);
                } catch (Exception e) {
                    LOGGER.error("Error in fetch listener for partition {} and session {}", topicPartition, sessionId, e);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Failed to submit fetch listener task for partition {} and session {}", topicPartition, sessionId, e);
        }
    }

    @Override
    public void onSessionClosed(TopicPartition topicPartition, int sessionId) {
        try {
            executor.execute(() -> {
                try {
                    delegate.onSessionClosed(topicPartition, sessionId);
                } catch (Exception e) {
                    LOGGER.error("Error in session close listener for partition {} and session {}", topicPartition, sessionId, e);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Failed to submit session close task for partition {} and session {}", topicPartition, sessionId, e);
        }
    }

    @Override
    public void onSessionClosedBatch(int sessionId, ImplicitLinkedHashCollection<CachedPartition> partitions) {
        try {
            executor.execute(() -> {
                for (CachedPartition partition : partitions) {
                    try {
                        delegate.onSessionClosed(new TopicPartition(partition.topic(), partition.partition()), sessionId);
                    } catch (Exception e) {
                        LOGGER.error(
                            "Error in session close listener for partition {}-{} and session {}",
                            partition.topic(),
                            partition.partition(),
                            sessionId,
                            e
                        );
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.error("Failed to submit batch session close task for session {}", sessionId, e);
        }
    }
}
