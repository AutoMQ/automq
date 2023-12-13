/*
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

package com.automq.stream.s3.metrics;

import io.opentelemetry.api.common.AttributeKey;

public class S3StreamMetricsConstant {
    public static final String UPLOAD_SIZE_METRIC_NAME = "upload_size_total";
    public static final String DOWNLOAD_SIZE_METRIC_NAME = "download_size_total";
    public static final String OPERATION_COUNT_METRIC_NAME = "operation_count_total";
    public static final String OPERATION_LATENCY_METRIC_NAME = "operation_latency";
    public static final String OBJECT_COUNT_METRIC_NAME = "object_count_total";
    public static final String OBJECT_STAGE_COST_METRIC_NAME = "object_stage_cost";
    public static final String OBJECT_UPLOAD_SIZE_METRIC_NAME = "object_upload_size";
    public static final String OBJECT_DOWNLOAD_SIZE_METRIC_NAME = "object_download_size";
    public static final String NETWORK_INBOUND_USAGE_METRIC_NAME = "network_inbound_usage_total";
    public static final String NETWORK_OUTBOUND_USAGE_METRIC_NAME = "network_outbound_usage_total";
    public static final String NETWORK_INBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME = "network_inbound_available_bandwidth";
    public static final String NETWORK_OUTBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME = "network_outbound_available_bandwidth";
    public static final String NETWORK_INBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME = "network_inbound_limiter_queue_size";
    public static final String NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME = "network_outbound_limiter_queue_size";
    public static final String ALLOCATE_BYTE_BUF_SIZE_METRIC_NAME = "allocate_byte_buf_size";
    public static final String READ_AHEAD_SIZE_METRIC_NAME = "read_ahead_size";
    public static final String AVAILABLE_INFLIGHT_READ_AHEAD_SIZE_METRIC_NAME = "available_inflight_read_ahead_size";
    public static final String COMPACTION_READ_SIZE_METRIC_NAME = "compaction_read_size_total";
    public static final String COMPACTION_WRITE_SIZE_METRIC_NAME = "compaction_write_size_total";
    public static final AttributeKey<String> LABEL_OPERATION_TYPE = AttributeKey.stringKey("operation_type");
    public static final AttributeKey<String> LABEL_OPERATION_NAME = AttributeKey.stringKey("operation_name");
    public static final AttributeKey<String> LABEL_APPEND_WAL_STAGE = AttributeKey.stringKey("stage");
    public static final AttributeKey<String> LABEL_CACHE_STATUS = AttributeKey.stringKey("status");
    public static final AttributeKey<String> LABEL_OBJECT_STAGE = AttributeKey.stringKey("stage");
    public static final AttributeKey<String> LABEL_ALLOCATE_BYTE_BUF_SOURCE = AttributeKey.stringKey("source");
}
