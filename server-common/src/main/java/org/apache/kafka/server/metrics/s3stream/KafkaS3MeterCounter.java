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
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.concurrent.TimeUnit;

public class KafkaS3MeterCounter implements Counter {
    private final com.yammer.metrics.core.Meter meter;

    public KafkaS3MeterCounter(MetricName metricName) {
        meter = KafkaYammerMetrics.defaultRegistry().newMeter(metricName, "s3Operations", TimeUnit.SECONDS);
    }

    @Override
    public void inc() {
        meter.mark(1);
    }

    @Override
    public void inc(long l) {
        meter.mark(l);
    }

    @Override
    public long count() {
        return meter.count();
    }

    public com.yammer.metrics.core.Meter meter() {
        return meter;
    }
}
