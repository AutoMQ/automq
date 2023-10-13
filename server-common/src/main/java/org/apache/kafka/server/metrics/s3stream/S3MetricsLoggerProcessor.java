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

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import org.slf4j.Logger;

public class S3MetricsLoggerProcessor implements MetricProcessor<S3MetricsLoggerProcessor.Context> {
    private static final Logger LOGGER = S3StreamMetricsLogger.logger("");

    @Override
    public void processMeter(MetricName name, Metered meter, Context context) throws Exception {
        LOGGER.info("{}:{}:{}, count={}, mean/m1Rate/m5Rate={}/{}/{}", name.getType(), name.getName(), name.getScope(),
                meter.count(), meter.meanRate(), meter.oneMinuteRate(), meter.fiveMinuteRate());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Context context) throws Exception {
        LOGGER.info("{}:{}:{}, count={}", name.getType(), name.getName(), name.getScope(), counter.count());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Context context) throws Exception {
        LOGGER.info("{}:{}:{}, mean/min/max={}/{}/{}", name.getType(), name.getName(), name.getScope(), histogram.mean(),
                histogram.min(), histogram.max());
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Context context) throws Exception {
        LOGGER.info("{}:{}:{}, count={}, mean/m1Rate/m5Rate={}/{}/{}", name.getType(), name.getName(), name.getScope(), timer.count(),
                timer.mean(), timer.oneMinuteRate(), timer.fiveMinuteRate());
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Context context) throws Exception {
        LOGGER.info("{}:{}:{}, value={}", name.getType(), name.getName(), name.getScope(), gauge.value());
    }

    public static class Context {

    }
}
