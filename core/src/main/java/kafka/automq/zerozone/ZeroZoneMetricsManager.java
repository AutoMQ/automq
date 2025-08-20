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

package kafka.automq.zerozone;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

public class ZeroZoneMetricsManager {
    private static final String PREFIX = "kafka_zonerouter_";

    private static final Cache<Integer, Attributes> ROUTER_OUT_ATTRIBUTES_CACHE = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build();
    private static final Cache<Integer, Attributes> ROUTER_IN_ATTRIBUTES_CACHE = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build();
    private static final LongCounter ROUTER_BYTES = Metrics.instance().counter(meter -> meter
        .counterBuilder(PREFIX + "router_bytes")
        .setUnit("bytes")
        .setDescription("Cross zone router bytes")
        .build());

    private static final Metrics.HistogramBundle ROUTER_LATENCY = Metrics.instance().histogram(PREFIX + "router_latency", "ZeroZone route latency", "nanoseconds");
    public static final DeltaHistogram APPEND_CHANNEL_LATENCY = ROUTER_LATENCY.histogram(MetricsLevel.DEBUG, Attributes.of(AttributeKey.stringKey("operation"), "out", AttributeKey.stringKey("stage"), "append_channel"));
    public static final DeltaHistogram PROXY_REQUEST_LATENCY = ROUTER_LATENCY.histogram(MetricsLevel.DEBUG, Attributes.of(AttributeKey.stringKey("operation"), "out", AttributeKey.stringKey("stage"), "proxy_request"));
    public static final DeltaHistogram GET_CHANNEL_LATENCY = ROUTER_LATENCY.histogram(MetricsLevel.DEBUG, Attributes.of(AttributeKey.stringKey("operation"), "in", AttributeKey.stringKey("stage"), "get_channel"));

    public static void recordRouterOutBytes(int toNodeId, int bytes) {
        try {
            ROUTER_BYTES.add(bytes, ROUTER_OUT_ATTRIBUTES_CACHE.get(toNodeId, () -> Attributes.of(AttributeKey.stringKey("type"), "out", AttributeKey.stringKey("peerNodeId"), Integer.toString(toNodeId))));
        } catch (ExecutionException e) {
            // suppress
        }
    }

    public static void recordRouterInBytes(int fromNodeId, int bytes) {
        try {
            ROUTER_BYTES.add(bytes, ROUTER_IN_ATTRIBUTES_CACHE.get(fromNodeId, () -> Attributes.of(AttributeKey.stringKey("type"), "in", AttributeKey.stringKey("peerNodeId"), Integer.toString(fromNodeId))));
        } catch (ExecutionException e) {
            // suppress
        }
    }
}
