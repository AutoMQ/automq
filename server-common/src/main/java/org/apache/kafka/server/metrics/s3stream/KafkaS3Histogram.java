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

import com.automq.stream.s3.metrics.Histogram;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

public class KafkaS3Histogram implements Histogram {
    private final com.yammer.metrics.core.Histogram histogram;

    public KafkaS3Histogram(MetricName metricName) {
        histogram = KafkaYammerMetrics.defaultRegistry().newHistogram(metricName, true);
    }

    @Override
    public void update(long l) {
        histogram.update(l);
    }

    @Override
    public long count() {
        return histogram.count();
    }

    @Override
    public double mean() {
        return histogram.mean();
    }

    public com.yammer.metrics.core.Histogram histogram() {
        return histogram;
    }
}
