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

package com.automq.shell.model;

import org.apache.kafka.common.Uuid;

public class StreamTags {

    public static class Topic {
        public static final String KEY = "0";

        public static String encode(Uuid topicId) {
            return topicId.toString();
        }

        public static Uuid decode(String tag) {
            return Uuid.fromString(tag);
        }
    }

    public static class Partition {
        public static final String KEY = "1";

        public static String encode(int partition) {
            return Integer.toHexString(partition);
        }

        public static int decode(String tag) {
            return Integer.parseInt(tag, 16);
        }

    }
}
