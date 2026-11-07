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

package kafka.automq.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.s3.StreamFencedException;

import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Static facade for the active {@link ElasticFailureHandler}.
 */
public final class ElasticFailureHandlers {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticFailureHandlers.class);
    private static final ElasticFailureHandler DEFAULT = new DefaultElasticFailureHandler();
    private static final AtomicReference<ElasticFailureHandler> HANDLER = new AtomicReference<>(DEFAULT);

    private ElasticFailureHandlers() {
    }

    public static void register(ElasticFailureHandler handler) {
        HANDLER.set(handler == null ? DEFAULT : handler);
    }

    public static void unregister(ElasticFailureHandler handler) {
        HANDLER.compareAndSet(handler, DEFAULT);
    }

    public static <T> CompletableFuture<T> readAsync(TopicPartition topicPartition, long startOffset,
                                                     ElasticFailureHandler.ReadOperation<T> readOperation) {
        return HANDLER.get().readAsync(topicPartition, startOffset, readOperation);
    }

    public static <T> T openWithRetry(TopicPartition topicPartition,
                                      ElasticFailureHandler.OpenOperation<T> openOperation) {
        return openWithRetry(topicPartition, openOperation, ElasticFailureHandler.OpenFailureContext.none());
    }

    public static <T> T openWithRetry(TopicPartition topicPartition,
                                      ElasticFailureHandler.OpenOperation<T> openOperation,
                                      ElasticFailureHandler.OpenFailureContext context) {
        return HANDLER.get().openWithRetry(topicPartition, openOperation, context);
    }

    private static final class DefaultElasticFailureHandler implements ElasticFailureHandler {
        @Override
        public <T> CompletableFuture<T> readAsync(TopicPartition topicPartition, long startOffset,
                                                  ReadOperation<T> readOperation) {
            return readOperation.read(startOffset);
        }

        @Override
        public <T> T openWithRetry(TopicPartition topicPartition, OpenOperation<T> openOperation,
                                   OpenFailureContext context) {
            while (true) {
                try {
                    return openOperation.open(false);
                } catch (Throwable e) {
                    Throwable cause = FutureUtil.cause(e);
                    if (cause instanceof StreamFencedException) {
                        throw (StreamFencedException) cause;
                    }
                    LOGGER.error("open {} failed, retry open after 1s", topicPartition, e);
                    Threads.sleep(1000);
                }
            }
        }
    }
}
