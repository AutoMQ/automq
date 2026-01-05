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

import io.opentelemetry.api.common.AttributeKey;

public class S3StreamKafkaMetricsConstants {
    public static final String AUTO_BALANCER_METRICS_TIME_DELAY_METRIC_NAME = "auto_balancer_metrics_time_delay";
    public static final String S3_OBJECT_COUNT_BY_STATE = "s3_object_count";
    public static final String S3_OBJECT_SIZE = "s3_object_size";
    public static final String STREAM_SET_OBJECT_NUM = "stream_set_object_num";
    public static final String STREAM_OBJECT_NUM = "stream_object_num";
    public static final String FETCH_LIMITER_PERMIT_NUM = "fetch_limiter_permit_num";
    public static final String FETCH_LIMITER_WAITING_TASK_NUM = "fetch_limiter_waiting_task_num";
    public static final String FETCH_PENDING_TASK_NUM = "fetch_pending_task_num";
    public static final String FETCH_LIMITER_TIMEOUT_COUNT = "fetch_limiter_timeout_count";
    public static final String FETCH_LIMITER_TIME = "fetch_limiter_time";
    public static final String LOG_APPEND_PERMIT_NUM = "log_append_permit_num";
    public static final String SLOW_BROKER_METRIC_NAME = "slow_broker_count";
    public static final String TOPIC_PARTITION_COUNT_METRIC_NAME = "topic_partition_count";

    public static final AttributeKey<String> LABEL_NODE_ID = AttributeKey.stringKey("node_id");
    public static final AttributeKey<String> LABEL_TOPIC_NAME = AttributeKey.stringKey("topic");
    public static final AttributeKey<String> LABEL_RACK_ID = AttributeKey.stringKey("rack");

    public static final AttributeKey<String> LABEL_OBJECT_STATE = AttributeKey.stringKey("state");
    public static final String S3_OBJECT_PREPARED_STATE = "prepared";
    public static final String S3_OBJECT_COMMITTED_STATE = "committed";
    public static final String S3_OBJECT_MARK_DESTROYED_STATE = "mark_destroyed";

    public static final AttributeKey<String> LABEL_FETCH_LIMITER_NAME = AttributeKey.stringKey("limiter_name");
    public static final String FETCH_LIMITER_FAST_NAME = "fast";
    public static final String FETCH_LIMITER_SLOW_NAME = "slow";

    public static final AttributeKey<String> LABEL_FETCH_EXECUTOR_NAME = AttributeKey.stringKey("executor_name");
    public static final String FETCH_EXECUTOR_FAST_NAME = "fast";
    public static final String FETCH_EXECUTOR_SLOW_NAME = "slow";
    public static final String FETCH_EXECUTOR_DELAYED_NAME = "delayed";

    public static final String PARTITION_STATUS_STATISTICS_METRIC_NAME = "partition_status_statistics";
    public static final AttributeKey<String> LABEL_STATUS = AttributeKey.stringKey("status");

    // Back Pressure
    public static final String BACK_PRESSURE_STATE_METRIC_NAME = "back_pressure_state";
    public static final AttributeKey<String> LABEL_BACK_PRESSURE_STATE = AttributeKey.stringKey("state");

    // License
    public static final String LICENSE_EXPIRY_TIMESTAMP_METRIC_NAME = "license_expiry_timestamp";
    public static final String LICENSE_SECONDS_REMAINING_METRIC_NAME = "license_seconds_remaining";
    public static final String NODE_VCPU_COUNT_METRIC_NAME = "node_vcpu_count";
}
