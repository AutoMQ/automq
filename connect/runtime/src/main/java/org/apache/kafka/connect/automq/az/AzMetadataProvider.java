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

package org.apache.kafka.connect.automq.az;

import java.util.Map;
import java.util.Optional;

/**
 * Pluggable provider for availability-zone metadata used to tune Kafka client configurations.
 */
public interface AzMetadataProvider {

    /**
     * Configure the provider with the worker properties. Implementations may cache values extracted from the
     * configuration map. This method is invoked exactly once during worker bootstrap.
     */
    default void configure(Map<String, String> workerProps) {
        // no-op
    }

    /**
     * @return the availability-zone identifier for the current node, if known.
     */
    default Optional<String> availabilityZoneId() {
        return Optional.empty();
    }
}
