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

package org.apache.kafka.server.metrics.s3stream;

import java.util.Map;

public class PartitionCountDistribution {
    private final int brokerId;
    private final String rack;
    private final Map<String, Integer> topicPartitionCount;

    public PartitionCountDistribution(int brokerId, String rack, Map<String, Integer> topicPartitionCount) {
        this.brokerId = brokerId;
        this.rack = rack;
        this.topicPartitionCount = topicPartitionCount;
    }

    public int brokerId() {
        return brokerId;
    }

    public String rack() {
        return rack;
    }

    public Map<String, Integer> topicPartitionCount() {
        return topicPartitionCount;
    }
}
