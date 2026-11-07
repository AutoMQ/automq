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

package org.apache.kafka.controller.stream;

public class TopicDeletion {

    public static final String TOPIC_DELETION_PREFIX = "__automq_topic_deletion/";

    public enum Status {
        PENDING(0),
        IN_PROGRESS(1),
        COMPLETE(2);

        private final int value;

        Status(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }

        public static Status forValue(int value) {
            switch (value) {
                case 0:
                    return PENDING;
                case 1:
                    return IN_PROGRESS;
                case 2:
                    return COMPLETE;
                default:
                    throw new IllegalArgumentException("Invalid value " + value);
            }
        }
    }

}
