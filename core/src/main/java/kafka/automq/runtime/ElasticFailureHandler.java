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

import java.util.concurrent.CompletableFuture;

/**
 * Extension point for Elastic log failure handling.
 *
 * <p>The OSS default handler preserves the existing retry behavior. Downstream distributions may
 * register a custom handler through {@link ElasticFailureHandlers} to add policy-specific handling
 * without exposing that policy in the OSS data path.</p>
 */
public interface ElasticFailureHandler {
    <T> CompletableFuture<T> readAsync(TopicPartition topicPartition, long startOffset,
                                       ReadOperation<T> readOperation);

    <T> T openWithRetry(TopicPartition topicPartition, OpenOperation<T> openOperation, OpenFailureContext context);

    interface ReadOperation<T> {
        CompletableFuture<T> read(long startOffset);
    }

    interface OpenOperation<T> {
        T open(boolean forceCleanShutdownRecovery) throws Exception;
    }

    interface CheckedRunnable {
        void run() throws Exception;
    }

    final class OpenFailureContext {
        private final CheckedRunnable recreateOperation;

        public OpenFailureContext(CheckedRunnable recreateOperation) {
            this.recreateOperation = recreateOperation;
        }

        public static OpenFailureContext none() {
            return new OpenFailureContext(null);
        }

        public static OpenFailureContext withRecreateOperation(CheckedRunnable recreateOperation) {
            return new OpenFailureContext(recreateOperation);
        }

        public boolean hasRecreateOperation() {
            return recreateOperation != null;
        }

        public void recreate() throws Exception {
            if (recreateOperation != null) {
                recreateOperation.run();
            }
        }
    }
}
