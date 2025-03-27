///*
// * Copyright 2024, AutoMQ HK Limited.
// *
// * The use of this file is governed by the Business Source License,
// * as detailed in the file "/LICENSE.S3Stream" included in this repository.
// *
// * As of the Change Date specified in that file, in accordance with
// * the Business Source License, use of this software will be governed
// * by the Apache License, Version 2.0
// */
//
//package com.automq.shell.metrics;
//
//import io.opentelemetry.proto.common.v1.AnyValue;
//import io.opentelemetry.proto.common.v1.InstrumentationScope;
//import io.opentelemetry.proto.common.v1.KeyValue;
//import io.opentelemetry.proto.metrics.v1.*;
//import io.opentelemetry.proto.resource.v1.Resource;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.zip.GZIPOutputStream;
//
//public class GzipCompressionRatioTest {
//
//    public static void main(String[] args) throws IOException {
//        // 构造一个大约 1M 左右的 ResourceMetrics 对象
//        ResourceMetrics resourceMetrics = buildLargeResourceMetrics();
//
//        // 序列化 ResourceMetrics 对象
//        byte[] serializedData = resourceMetrics.toByteArray();
//        int originalSize = serializedData.length;
//        System.out.println("原始数据大小: " + originalSize + " 字节");
//
//        // 使用 Gzip 压缩序列化后的数据
//        byte[] compressedData = compressWithGzip(serializedData);
//        int compressedSize = compressedData.length;
//        System.out.println("压缩后数据大小: " + compressedSize + " 字节");
//
//        // 计算压缩比
//        double compressionRatio = (double) originalSize / compressedSize;
//        System.out.println("压缩比: " + compressionRatio);
//    }
//
//    private static ResourceMetrics buildLargeResourceMetrics() {
//        ResourceMetrics.Builder resourceMetricsBuilder = ResourceMetrics.newBuilder();
//        Resource.Builder resourceBuilder = Resource.newBuilder();
//        // 为资源添加一些属性
//        for (int i = 0; i < 1000; i++) {
//            resourceBuilder.addAttributes(KeyValue.newBuilder()
//                   .setKey("attribute_key_" + i)
//                   .setValue(AnyValue.newBuilder().setStringValue("attribute_value_" + i).build())
//                   .build());
//        }
//        resourceMetricsBuilder.setResource(resourceBuilder.build());
//
//        ScopeMetrics.Builder scopeMetricsBuilder = ScopeMetrics.newBuilder();
//        InstrumentationScope.Builder scopeBuilder = InstrumentationScope.newBuilder();
//        scopeBuilder.setName("test_scope");
//        scopeMetricsBuilder.setScope(scopeBuilder.build());
//
//        // 构造多个指标
//        for (int i = 0; i < 1000; i++) {
//            Metric.Builder metricBuilder = Metric.newBuilder();
//            metricBuilder.setName("metric_" + i);
//            metricBuilder.setDescription("This is metric " + i);
//            metricBuilder.setUnit("count");
//
//            Gauge.Builder gaugeBuilder = Gauge.newBuilder();
//            for (int j = 0; j < 10; j++) {
//                NumberDataPoint.Builder dataPointBuilder = NumberDataPoint.newBuilder();
//                dataPointBuilder.setStartTimeUnixNano(1600000000000000000L + j * 1000000000L);
//                dataPointBuilder.setTimeUnixNano(1600000000000000000L + (j + 1) * 1000000000L);
//                dataPointBuilder.setAsInt(i * j);
//                gaugeBuilder.addDataPoints(dataPointBuilder.build());
//            }
//            metricBuilder.setGauge(gaugeBuilder.build());
//            scopeMetricsBuilder.addMetrics(metricBuilder.build());
//        }
//        resourceMetricsBuilder.addScopeMetrics(scopeMetricsBuilder.build());
//
//        return resourceMetricsBuilder.build();
//    }
//
//    private static byte[] compressWithGzip(byte[] data) throws IOException {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
//        try (GZIPOutputStream gzipOut = new GZIPOutputStream(bos)) {
//            gzipOut.write(data);
//        }
//        return bos.toByteArray();
//    }
//}
