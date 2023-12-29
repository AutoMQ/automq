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

import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AttributesCache {
    public static final AttributesCache INSTANCE = new AttributesCache();
    private final Map<String, Attributes> attributesMap = new ConcurrentHashMap<>();
    private Attributes defaultAttributes = null;

    private AttributesCache() {
    }

    public void setDefaultAttributes(Attributes defaultAttributes) {
        this.defaultAttributes = defaultAttributes;
    }

    public Attributes defaultAttributes() {
        return defaultAttributes == null ? Attributes.builder().build() : defaultAttributes;
    }

    public Attributes getAttributes(S3Operation operation, long size, boolean isSuccess) {
        String sizeLabelName = getObjectBucketLabel(size);
        String key = operation.getUniqueKey() + "-" + sizeLabelName + "-" + isSuccess;
        return attributesMap.computeIfAbsent(key, k -> buildAttributes(operation, sizeLabelName, isSuccess));
    }

    public Attributes getAttributes(S3Stage stage) {
        String key = stage.getOperation().getUniqueKey() + "-" + stage.getName();
        return attributesMap.computeIfAbsent(key, k -> buildAttributes(stage));
    }

    public Attributes getAttributes(S3Operation operation, String status) {
        String key = operation.getUniqueKey() + "-" + status;
        return attributesMap.computeIfAbsent(key, k -> buildAttributes(operation, status));
    }

    public Attributes getAttributes(String source) {
        String key = "AllocateByteBuf-" + source;
        return attributesMap.computeIfAbsent(key, k -> buildAttributes(source));
    }

    public Attributes getAttributes(S3ObjectStage stage) {
        String key = "S3ObjectStage-" + stage.getName();
        return attributesMap.computeIfAbsent(key, k -> buildAttributes(stage));
    }

    String getObjectBucketLabel(long objectSize) {
        int index = (int) Math.ceil(Math.log((double) objectSize / (16 * 1024)) / Math.log(2));
        index = Math.min(S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES.length - 1, Math.max(0, index));
        return S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES[index];
    }

    private Attributes buildAttributes(S3Operation operation, String sizeLabelName, boolean isSuccess) {
        AttributesBuilder attributesBuilder = defaultAttributes().toBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .put(S3StreamMetricsConstant.LABEL_STATUS, isSuccess ? "success" : "failed");
        if (operation == S3Operation.GET_OBJECT || operation == S3Operation.PUT_OBJECT || operation == S3Operation.UPLOAD_PART) {
            attributesBuilder.put(S3StreamMetricsConstant.LABEL_SIZE_NAME, sizeLabelName);
        }
        return attributesBuilder.build();
    }

    private Attributes buildAttributes(S3Stage stage) {
        return defaultAttributes().toBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, stage.getOperation().getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, stage.getOperation().getName())
                .put(S3StreamMetricsConstant.LABEL_STAGE, stage.getName())
                .build();
    }

    private Attributes buildAttributes(S3Operation operation, String status) {
        return defaultAttributes().toBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .put(S3StreamMetricsConstant.LABEL_STATUS, status)
                .build();
    }

    private Attributes buildAttributes(S3ObjectStage stage) {
        return defaultAttributes().toBuilder()
                .put(S3StreamMetricsConstant.LABEL_STAGE, stage.getName())
                .build();
    }

    private Attributes buildAttributes(String source) {
        return defaultAttributes().toBuilder()
                .put(S3StreamMetricsConstant.LABEL_ALLOCATE_BYTE_BUF_SOURCE, source)
                .build();
    }

}
