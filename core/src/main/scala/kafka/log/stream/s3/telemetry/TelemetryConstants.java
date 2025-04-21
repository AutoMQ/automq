/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
