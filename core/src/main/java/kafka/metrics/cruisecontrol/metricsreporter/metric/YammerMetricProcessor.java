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
/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.metrics.cruisecontrol.metricsreporter.metric;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import kafka.metrics.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


/**
 * A Yammer metric processor that process the yammer metrics. Currently all the interested metrics are of type
 * Meter (BytesInRate, BytesOutRate) or Gauge (Partition Size).
 */
public class YammerMetricProcessor implements MetricProcessor<YammerMetricProcessor.Context> {
    private static final Logger LOG = LoggerFactory.getLogger(YammerMetricProcessor.class);

    @Override
    public void processMeter(MetricName metricName, Metered metered, Context context) {
        if (MetricsUtils.isInterested(metricName)) {
            LOG.trace("Processing metric {} of type Meter.", metricName);
            double value;
            if (context.reportingInterval().toMillis() <= TimeUnit.MINUTES.toMillis(1)) {
                value = metered.oneMinuteRate();
            } else if (context.reportingInterval().toMillis() <= TimeUnit.MINUTES.toMillis(5)) {
                value = metered.fiveMinuteRate();
            } else {
                value = metered.fifteenMinuteRate();
            }
            CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                value);
            context.reporter().sendCruiseControlMetric(ccm);
        }
    }

    @Override
    public void processCounter(MetricName metricName, Counter counter, Context context) {
        if (MetricsUtils.isInterested(metricName)) {
            LOG.warn("Not processing metric {} of type Counter.", metricName);
        }
    }

    @Override
    public void processHistogram(MetricName metricName, Histogram histogram, Context context) {
        if (MetricsUtils.isInterested(metricName)) {
            LOG.trace("Processing metric {} of type Histogram.", metricName);
            // Get max metric value
            CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                histogram.max(),
                MetricsUtils.ATTRIBUTE_MAX);
            context.reporter().sendCruiseControlMetric(ccm);

            // Get mean metric value
            ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                histogram.mean(),
                MetricsUtils.ATTRIBUTE_MEAN);
            context.reporter().sendCruiseControlMetric(ccm);

            Snapshot snapshot = histogram.getSnapshot();
            // Get 50th percentile (i.e. median) metric value
            ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                snapshot.getMedian(),
                MetricsUtils.ATTRIBUTE_50TH_PERCENTILE);
            context.reporter().sendCruiseControlMetric(ccm);

            // Get 999th percentile metric value
            ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                snapshot.get999thPercentile(),
                MetricsUtils.ATTRIBUTE_999TH_PERCENTILE);
            context.reporter().sendCruiseControlMetric(ccm);
        }
    }

    @Override
    public void processTimer(MetricName metricName, Timer timer, Context context) {
        if (MetricsUtils.isInterested(metricName)) {
            LOG.trace("Processing metric {} of type Timer.", metricName);

            CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                timer.fiveMinuteRate());
            context.reporter().sendCruiseControlMetric(ccm);
            // Get max metric value
            ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                timer.max(),
                MetricsUtils.ATTRIBUTE_MAX);
            context.reporter().sendCruiseControlMetric(ccm);
            // Get mean metric value
            ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                timer.mean(),
                MetricsUtils.ATTRIBUTE_MEAN);
            context.reporter().sendCruiseControlMetric(ccm);

            Snapshot snapshot = timer.getSnapshot();
            // Get 50th percentile (i.e. median) metric value
            ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                snapshot.getMedian(),
                MetricsUtils.ATTRIBUTE_50TH_PERCENTILE);
            context.reporter().sendCruiseControlMetric(ccm);

            // Get 999th percentile metric value
            ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                snapshot.get999thPercentile(),
                MetricsUtils.ATTRIBUTE_999TH_PERCENTILE);
            context.reporter().sendCruiseControlMetric(ccm);
        }
    }

    @Override
    public void processGauge(MetricName metricName, Gauge<?> gauge, Context context) {
        if (MetricsUtils.isInterested(metricName)) {
            LOG.trace("Processing metric {} of type Gauge.", metricName);
            if (!(gauge.value() instanceof Number)) {
                throw new IllegalStateException(String.format("The value of yammer metric %s is %s, which is not a number",
                    metricName, gauge.value()));
            }
            CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                context.brokerId(),
                metricName,
                ((Number) gauge.value()).doubleValue());
            context.reporter().sendCruiseControlMetric(ccm);
        }
    }

    public static final class Context {
        private final CruiseControlMetricsReporter reporter;
        private final long time;
        private final int brokerId;
        private final Duration reportingInterval;

        public Context(CruiseControlMetricsReporter reporter, long time, int brokerId, long reportingIntervalMs) {
            this.reporter = reporter;
            this.time = time;
            this.brokerId = brokerId;
            reportingInterval = Duration.ofMillis(reportingIntervalMs);
        }

        private CruiseControlMetricsReporter reporter() {
            return reporter;
        }

        private long time() {
            return time;
        }

        private int brokerId() {
            return brokerId;
        }

        private Duration reportingInterval() {
            return reportingInterval;
        }
    }
}
