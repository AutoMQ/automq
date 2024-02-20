/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry.metrics;

import io.opentelemetry.api.common.AttributeKey;

public class S3StreamKafkaMetricsConstants {
    public static final String AUTO_BALANCER_METRICS_TIME_DELAY_METRIC_NAME = "auto_balancer_metrics_time_delay";
    public static final AttributeKey<String> LABEL_NODE_ID = AttributeKey.stringKey("node_id");
}
