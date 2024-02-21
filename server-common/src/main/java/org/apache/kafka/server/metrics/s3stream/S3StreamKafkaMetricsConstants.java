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

package org.apache.kafka.server.metrics.s3stream;

import io.opentelemetry.api.common.AttributeKey;

public class S3StreamKafkaMetricsConstants {
    public static final String AUTO_BALANCER_METRICS_TIME_DELAY_METRIC_NAME = "auto_balancer_metrics_time_delay";
    public static final String S3_OBJECT_COUNT_BY_STATE = "s3_object_count";
    public static final String S3_OBJECT_SIZE = "s3_object_size";
    public static final String STREAM_SET_OBJECT_NUM = "stream_set_object_num";
    public static final String STREAM_OBJECT_NUM = "stream_object_num";
    public static final String S3_OBJECT_PREPARED_STATE = "prepared";
    public static final String S3_OBJECT_COMMITTED_STATE = "committed";
    public static final String S3_OBJECT_MARK_DESTROYED_STATE = "mark_destroyed";
    public static final AttributeKey<String> LABEL_NODE_ID = AttributeKey.stringKey("node_id");
    public static final AttributeKey<String> LABEL_OBJECT_STATE = AttributeKey.stringKey("state");
}
