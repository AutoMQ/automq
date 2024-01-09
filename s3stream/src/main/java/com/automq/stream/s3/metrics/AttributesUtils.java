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

public class AttributesUtils {

    public static Attributes buildAttributes(S3Operation operation) {
        return Attributes.builder()
            .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
            .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
            .build();
    }

    public static Attributes buildAttributes(S3Operation operation, String status) {
        return Attributes.builder()
            .putAll(buildAttributes(operation))
            .put(S3StreamMetricsConstant.LABEL_STATUS, status)
            .build();
    }

    public static Attributes buildAttributes(S3Stage stage) {
        return Attributes.builder()
            .putAll(buildAttributes(stage.getOperation()))
            .put(S3StreamMetricsConstant.LABEL_STAGE, stage.getName())
            .build();
    }

    public static Attributes buildAttributes(S3Operation operation, String status, String sizeLabelName) {
        return Attributes.builder()
            .putAll(buildAttributes(operation, status))
            .put(S3StreamMetricsConstant.LABEL_SIZE_NAME, sizeLabelName)
            .build();
    }

    public static Attributes buildAttributes(S3ObjectStage objectStage) {
        return Attributes.builder()
            .put(S3StreamMetricsConstant.LABEL_STAGE, objectStage.getName())
            .build();
    }

    public static Attributes buildAttributes(String source) {
        return Attributes.builder()
            .put(S3StreamMetricsConstant.LABEL_ALLOCATE_BYTE_BUF_SOURCE, source)
            .build();
    }

    public static String getObjectBucketLabel(long objectSize) {
        int index = (int) Math.ceil(Math.log((double) objectSize / (16 * 1024)) / Math.log(2));
        index = Math.min(S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES.length - 1, Math.max(0, index));
        return S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES[index];
    }
}
