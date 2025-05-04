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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

public class S3ObjectStats {
    private static volatile S3ObjectStats instance = null;

    public final CounterMetric objectNumInTotalStats = S3StreamMetricsManager.buildObjectNumMetric();
    public final HistogramMetric objectStageUploadPartStats = S3StreamMetricsManager
            .buildObjectStageCostMetric(MetricsLevel.DEBUG, S3ObjectStage.UPLOAD_PART);
    public final HistogramMetric objectStageReadyCloseStats = S3StreamMetricsManager
            .buildObjectStageCostMetric(MetricsLevel.DEBUG, S3ObjectStage.READY_CLOSE);
    public final HistogramMetric objectStageTotalStats = S3StreamMetricsManager
            .buildObjectStageCostMetric(MetricsLevel.DEBUG, S3ObjectStage.TOTAL);
    public final HistogramMetric objectUploadSizeStats = S3StreamMetricsManager.buildObjectUploadSizeMetric(MetricsLevel.DEBUG);

    private S3ObjectStats() {
    }

    public static S3ObjectStats getInstance() {
        if (instance == null) {
            synchronized (S3ObjectStats.class) {
                if (instance == null) {
                    instance = new S3ObjectStats();
                }
            }
        }
        return instance;
    }
}
