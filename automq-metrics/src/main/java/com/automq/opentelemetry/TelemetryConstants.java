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

package com.automq.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;

/**
 * Constants for telemetry, including configuration keys, attribute keys, and default values.
 */
public class TelemetryConstants {

    //################################################################
    // Service and Resource Attributes
    //################################################################
    public static final String SERVICE_NAME_KEY = "service.name";
    public static final String SERVICE_INSTANCE_ID_KEY = "service.instance.id";
    public static final String HOST_NAME_KEY = "host.name";
    public static final String TELEMETRY_SCOPE_NAME = "automq_for_kafka";
    
    /**
     * The cardinality limit for any single metric.
     */
    public static final String METRIC_CARDINALITY_LIMIT_KEY = "automq.telemetry.metric.cardinality.limit";
    public static final int DEFAULT_METRIC_CARDINALITY_LIMIT = 20000;

    //################################################################
    // Prometheus specific Attributes, for compatibility
    //################################################################
    public static final String PROMETHEUS_JOB_KEY = "job";
    public static final String PROMETHEUS_INSTANCE_KEY = "instance";

    //################################################################
    // Custom Kafka-related Attribute Keys
    //################################################################
    public static final AttributeKey<Long> START_OFFSET_KEY = AttributeKey.longKey("startOffset");
    public static final AttributeKey<Long> END_OFFSET_KEY = AttributeKey.longKey("endOffset");
}
