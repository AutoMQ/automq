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

package com.automq.stream.s3.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.AttributeKey;

public class S3StreamMetricsConstant {
    // value = 16KB * 2^i
    public static final String[] OBJECT_SIZE_BUCKET_NAMES = {
        "16KB",
        "32KB",
        "64KB",
        "128KB",
        "256KB",
        "512KB",
        "1MB",
        "2MB",
        "4MB",
        "8MB",
        "16MB",
        "32MB",
        "64MB",
        "128MB",
        "inf"};
    public static final List<Long> LATENCY_BOUNDARIES = List.of(
        TimeUnit.MICROSECONDS.toNanos(1),
        TimeUnit.MICROSECONDS.toNanos(10),
        TimeUnit.MICROSECONDS.toNanos(100),
        TimeUnit.MILLISECONDS.toNanos(1),
        TimeUnit.MILLISECONDS.toNanos(3),
        TimeUnit.MILLISECONDS.toNanos(5),
        TimeUnit.MILLISECONDS.toNanos(7),
        TimeUnit.MILLISECONDS.toNanos(10),
        TimeUnit.MILLISECONDS.toNanos(20),
        TimeUnit.MILLISECONDS.toNanos(30),
        TimeUnit.MILLISECONDS.toNanos(40),
        TimeUnit.MILLISECONDS.toNanos(50),
        TimeUnit.MILLISECONDS.toNanos(60),
        TimeUnit.MILLISECONDS.toNanos(70),
        TimeUnit.MILLISECONDS.toNanos(80),
        TimeUnit.MILLISECONDS.toNanos(90),
        TimeUnit.MILLISECONDS.toNanos(100),
        TimeUnit.MILLISECONDS.toNanos(200),
        TimeUnit.MILLISECONDS.toNanos(500),
        TimeUnit.SECONDS.toNanos(1),
        TimeUnit.SECONDS.toNanos(3),
        TimeUnit.SECONDS.toNanos(5),
        TimeUnit.SECONDS.toNanos(10),
        TimeUnit.SECONDS.toNanos(30),
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(3),
        TimeUnit.MINUTES.toNanos(5)
    );

    public static final String UPLOAD_SIZE_METRIC_NAME = "upload_size";
    public static final String DOWNLOAD_SIZE_METRIC_NAME = "download_size";
    public static final String OPERATION_COUNT_METRIC_NAME = "operation_count";
    public static final String OPERATION_LATENCY_METRIC_NAME = "operation_latency";
    public static final String OBJECT_COUNT_METRIC_NAME = "object_count";
    public static final String OBJECT_STAGE_COST_METRIC_NAME = "object_stage_cost";
    public static final String NETWORK_INBOUND_USAGE_METRIC_NAME = "network_inbound_usage";
    public static final String NETWORK_OUTBOUND_USAGE_METRIC_NAME = "network_outbound_usage";
    public static final String NETWORK_INBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME = "network_inbound_available_bandwidth";
    public static final String NETWORK_OUTBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME = "network_outbound_available_bandwidth";
    public static final String NETWORK_INBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME = "network_inbound_limiter_queue_size";
    public static final String NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME = "network_outbound_limiter_queue_size";
    public static final String NETWORK_INBOUND_LIMITER_QUEUE_TIME_METRIC_NAME = "network_inbound_limiter_queue_time";
    public static final String NETWORK_OUTBOUND_LIMITER_QUEUE_TIME_METRIC_NAME = "network_outbound_limiter_queue_time";
    public static final String READ_AHEAD_SIZE_METRIC_NAME = "read_ahead_size";
    public static final String READ_AHEAD_STAGE_TIME_METRIC_NAME = "read_ahead_stage_time";
    public static final String PENDING_STREAM_APPEND_LATENCY_METRIC_NAME = "pending_stream_append_latency";
    public static final String PENDING_STREAM_FETCH_LATENCY_METRIC_NAME = "pending_stream_fetch_latency";
    public static final String SUM_METRIC_NAME_SUFFIX = "_sum";
    public static final String COUNT_METRIC_NAME_SUFFIX = "_count";
    public static final String P50_METRIC_NAME_SUFFIX = "_50p";
    public static final String P99_METRIC_NAME_SUFFIX = "_99p";
    public static final String MAX_METRIC_NAME_SUFFIX = "_max";
    public static final String WAL_START_OFFSET = "wal_start_offset";
    public static final String WAL_PENDING_UPLOAD_BYTES = "wal_pending_upload_bytes";
    public static final String WAL_TRIMMED_OFFSET = "wal_trimmed_offset";
    public static final String DELTA_WAL_CACHE_SIZE = "delta_wal_cache_size";
    public static final String BLOCK_CACHE_SIZE = "block_cache_size";
    public static final String AVAILABLE_INFLIGHT_READ_AHEAD_SIZE_METRIC_NAME = "available_inflight_read_ahead_size";
    public static final String READ_AHEAD_QUEUE_TIME_METRIC_NAME = "read_ahead_limiter_queue_time";
    public static final String AVAILABLE_S3_INFLIGHT_READ_QUOTA_METRIC_NAME = "available_s3_inflight_read_quota";
    public static final String AVAILABLE_S3_INFLIGHT_WRITE_QUOTA_METRIC_NAME = "available_s3_inflight_write_quota";
    public static final String INFLIGHT_WAL_UPLOAD_TASKS_COUNT_METRIC_NAME = "inflight_wal_upload_tasks_count";
    public static final String COMPACTION_READ_SIZE_METRIC_NAME = "compaction_read_size";
    public static final String COMPACTION_WRITE_SIZE_METRIC_NAME = "compaction_write_size";
    public static final String ASYNC_CACHE_EVICT_COUNT_METRIC_NAME = "async_cache_evict";
    public static final String ASYNC_CACHE_HIT_COUNT_METRIC_NAME = "async_cache_hit";
    public static final String ASYNC_CACHE_MISS_COUNT_METRIC_NAME = "async_cache_miss";
    public static final String ASYNC_CACHE_PUT_COUNT_METRIC_NAME = "async_cache_put";
    public static final String ASYNC_CACHE_POP_COUNT_METRIC_NAME = "async_cache_pop";
    public static final String ASYNC_CACHE_OVERWRITE_COUNT_METRIC_NAME = "async_cache_overwrite";
    public static final String ASYNC_CACHE_REMOVE_NOT_COMPLETE_COUNT_METRIC_NAME = "async_cache_remove_item_not_complete";
    public static final String ASYNC_CACHE_REMOVE_COMPLETE_COUNT_METRIC_NAME = "async_cache_remove_item_complete";
    public static final String ASYNC_CACHE_ITEM_COMPLETE_EXCEPTIONALLY_COUNT_METRIC_NAME = "async_cache_item_complete_exceptionally";
    public static final String ASYNC_CACHE_ITEM_NUMBER_METRIC_NAME = "async_cache_item_count";
    public static final String ASYNC_CACHE_ITEM_SIZE_NAME = "async_cache_item_size";
    public static final String ASYNC_CACHE_ITEM_MAX_SIZE_NAME = "async_cache_max_size";
    public static final String BUFFER_ALLOCATED_MEMORY_SIZE_METRIC_NAME = "buffer_allocated_memory_size";
    public static final String BUFFER_USED_MEMORY_SIZE_METRIC_NAME = "buffer_used_memory_size";
    public static final String READ_S3_LIMITER_TIME_METRIC_NAME = "read_s3_limiter_time";
    public static final String WRITE_S3_LIMITER_TIME_METRIC_NAME = "write_s3_limiter_time";
    public static final String GET_INDEX_TIME_METRIC_NAME = "get_index_time";
    public static final String READ_BLOCK_CACHE_METRIC_NAME = "read_block_cache_stage_time";
    public static final String READ_BLOCK_CACHE_THROUGHPUT_METRIC_NAME = "block_cache_ops_throughput";
    public static final String COMPACTION_DELAY_TIME_METRIC_NAME = "compaction_delay_time";
    public static final String GET_OBJECTS_TIME_METRIC_NAME = "get_objects_time";
    public static final String OBJECTS_SEARCH_COUNT_METRIC_NAME = "objects_search_count";
    public static final String LOCAL_STREAM_RANGE_INDEX_CACHE_SIZE_METRIC_NAME = "local_stream_range_index_cache_size";
    public static final String LOCAL_STREAM_RANGE_INDEX_CACHE_STREAM_NUM_METRIC_NAME = "local_stream_range_index_cache_stream_num";
    public static final String NODE_RANGE_INDEX_CACHE_OPERATION_COUNT_METRIC_NAME = "node_range_index_cache_operation_count";
    public static final AttributeKey<String> LABEL_OPERATION_TYPE = AttributeKey.stringKey("operation_type");
    public static final AttributeKey<String> LABEL_OPERATION_NAME = AttributeKey.stringKey("operation_name");
    public static final AttributeKey<String> LABEL_SIZE_NAME = AttributeKey.stringKey("size");
    public static final AttributeKey<String> LABEL_STAGE = AttributeKey.stringKey("stage");
    public static final AttributeKey<String> LABEL_STATUS = AttributeKey.stringKey("status");
    public static final AttributeKey<String> LABEL_TYPE = AttributeKey.stringKey("type");
    public static final AttributeKey<String> LABEL_INDEX = AttributeKey.stringKey("index");
    public static final AttributeKey<String> LABEL_CACHE_NAME = AttributeKey.stringKey("cacheName");
    public static final String LABEL_STATUS_SUCCESS = "success";
    public static final String LABEL_STATUS_FAILED = "failed";
    public static final String LABEL_STATUS_HIT = "hit";
    public static final String LABEL_STATUS_MISS = "miss";
    public static final String LABEL_STATUS_UPDATE = "update";
    public static final String LABEL_STATUS_INVALIDATE = "invalidate";
    public static final String LABEL_STATUS_SYNC = "sync";
    public static final String LABEL_STATUS_ASYNC = "async";

    public static final String LABEL_STAGE_GET_INDICES = "get_indices";
    public static final String LABEL_STAGE_THROTTLE = "throttle";
    public static final String LABEL_STAGE_READ_S3 = "read_s3";
    public static final String LABEL_STAGE_PUT_BLOCK_CACHE = "put_block_cache";
    public static final String LABEL_STAGE_WAIT_INFLIGHT = "wait_inflight";
    public static final String LABEL_STAGE_READ_CACHE = "read_cache";
    public static final String LABEL_STAGE_READ_AHEAD = "read_ahead";
    public static final String LABEL_STAGE_GET_OBJECTS = "get_objects";
    public static final String LABEL_STAGE_FIND_INDEX = "find_index";
    public static final String LABEL_STAGE_COMPUTE = "compute";

    // Broker Quota
    public static final String BROKER_QUOTA_LIMIT_METRIC_NAME = "broker_quota_limit";
    public static final AttributeKey<String> LABEL_BROKER_QUOTA_TYPE = AttributeKey.stringKey("type");
}
