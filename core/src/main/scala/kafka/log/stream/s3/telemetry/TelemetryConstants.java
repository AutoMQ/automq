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

package kafka.log.stream.s3.telemetry;

import io.opentelemetry.api.common.AttributeKey;

public class TelemetryConstants {
    // The maximum number of unique attribute combinations for a single metric
    public static final int CARDINALITY_LIMIT = 20000;
    public static final String COMMON_JMX_YAML_CONFIG_PATH = "/jmx/rules/common.yaml";
    public static final String BROKER_JMX_YAML_CONFIG_PATH = "/jmx/rules/broker.yaml";
    public static final String CONTROLLER_JMX_YAML_CONFIG_PATH = "/jmx/rules/controller.yaml";
    public static final String TELEMETRY_SCOPE_NAME = "automq_for_kafka";
    public static final String KAFKA_METRICS_PREFIX = "kafka_stream_";
    public static final String KAFKA_WAL_METRICS_PREFIX = "kafka_wal_";
    public static final AttributeKey<Long> STREAM_ID_NAME = AttributeKey.longKey("streamId");
    public static final AttributeKey<Long> START_OFFSET_NAME = AttributeKey.longKey("startOffset");
    public static final AttributeKey<Long> END_OFFSET_NAME = AttributeKey.longKey("endOffset");
    public static final AttributeKey<Long> MAX_BYTES_NAME = AttributeKey.longKey("maxBytes");
}
