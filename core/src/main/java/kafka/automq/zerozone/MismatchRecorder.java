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

package kafka.automq.zerozone;

import kafka.automq.interceptor.ClientIdMetadata;

import org.apache.kafka.common.internals.Topic;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

class MismatchRecorder {
    private static final Logger LOGGER = LoggerFactory.getLogger(MismatchRecorder.class);
    private static final int MAX_RECORD_SIZE = 16;
    private ConcurrentMap<String, ClientIdMetadata> topic2clientId = new ConcurrentHashMap<>(MAX_RECORD_SIZE);
    private ConcurrentMap<String, ClientIdMetadata> topic2clientId2 = new ConcurrentHashMap<>(MAX_RECORD_SIZE);

    private static final MismatchRecorder INSTANCE = new MismatchRecorder();

    private MismatchRecorder() {
        Threads.COMMON_SCHEDULER.scheduleWithFixedDelay(this::logAndReset, 1, 1, TimeUnit.MINUTES);
    }

    public static MismatchRecorder instance() {
        return INSTANCE;
    }

    public void record(String topic, ClientIdMetadata clientId) {
        if (topic2clientId.size() >= MAX_RECORD_SIZE) {
            return;
        }
        if (topic.equals(Topic.AUTO_BALANCER_METRICS_TOPIC_NAME)) {
            return;
        }
        topic2clientId.putIfAbsent(topic, clientId);
    }

    private void logAndReset() {
        if (topic2clientId.isEmpty()) {
            return;
        }
        ConcurrentMap<String, ClientIdMetadata> logMap = topic2clientId;
        topic2clientId = topic2clientId2;
        topic2clientId2 = logMap;
        LOGGER.warn("[RACK_AWARE_MISMATCH],{}", logMap);
        logMap.clear();
    }

}
