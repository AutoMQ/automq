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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

final class BlockCacheMetrics {
    private static final AttributeKey<String> LABEL_OPS = AttributeKey.stringKey("ops");

    private static final Metrics.LongCounterBundle BLOCK_CACHE_OPS_THROUGHPUT = Metrics.instance()
        .longCounter("kafka_stream_block_cache_ops_throughput", "Block cache operation throughput", "bytes");

    static final Metrics.LongCounterBundle.LongCounter READ_S3_THROUGHPUT = blockCacheOpsThroughput("read_s3");
    static final Metrics.LongCounterBundle.LongCounter BLOCK_MISS_THROUGHPUT = blockCacheOpsThroughput("block_miss");
    static final Metrics.LongCounterBundle.LongCounter BLOCK_EVICT_THROUGHPUT = blockCacheOpsThroughput("block_evict");
    static final Metrics.LongCounterBundle.LongCounter READ_STREAM_THROUGHPUT = blockCacheOpsThroughput("read_stream");
    static final Metrics.LongCounterBundle.LongCounter READAHEAD_THROUGHPUT = blockCacheOpsThroughput("readahead");

    private BlockCacheMetrics() {
    }

    private static Metrics.LongCounterBundle.LongCounter blockCacheOpsThroughput(String ops) {
        return BLOCK_CACHE_OPS_THROUGHPUT.register(MetricsLevel.INFO, Attributes.of(LABEL_OPS, ops));
    }
}
