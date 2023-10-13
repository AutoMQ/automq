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

package org.apache.kafka.server.metrics.s3stream;

import com.automq.stream.s3.metrics.Counter;
import com.automq.stream.s3.metrics.Histogram;
import com.automq.stream.s3.metrics.S3StreamMetricsGroup;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.LinkedHashMap;
import java.util.Map;

public class KafkaS3StreamMetricsGroup implements S3StreamMetricsGroup {
    @Override
    public Counter newCounter(String type, String name, Map<String, String> tags) {
        return new KafkaS3MeterCounter(metricName(type, name, tags));
    }

    @Override
    public Histogram newHistogram(String type, String name, Map<String, String> tags) {
        return new KafkaS3Histogram(metricName(type, name, tags));
    }

    private MetricName metricName(String type, String name, Map<String, String> tags) {
        String group = KafkaS3StreamMetricsGroup.class.getPackageName();
        return KafkaYammerMetrics.getMetricName(group, type, name, new LinkedHashMap<>(tags));
    }
}
