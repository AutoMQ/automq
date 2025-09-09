package com.automq.opentelemetry.exporter.remotewrite;

import com.automq.opentelemetry.telemetry.RemoteWrite;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.metrics.data.ValueAtQuantile;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RemoteWriteRequestMarshaller {
    private final Map<PromLabels, PromTimeSeries> timeSeriesMap = new HashMap<>();

    public Collection<RemoteWrite.TimeSeries> fromMetrics(Collection<MetricData> metrics) {
        for (MetricData metric : metrics) {
            switch (metric.getType()) {
                case LONG_GAUGE:
                    addLongGauge(metric);
                    break;
                case DOUBLE_GAUGE:
                    addDoubleGauge(metric);
                    break;
                case LONG_SUM:
                    addLongSum(metric);
                    break;
                case DOUBLE_SUM:
                    addDoubleSum(metric);
                    break;
                case SUMMARY:
                    addSummary(metric);
                    break;
                case HISTOGRAM:
                    addHistogram(metric);
                    break;
                case EXPONENTIAL_HISTOGRAM:
                    throw new UnsupportedOperationException("Unsupported metric type: " + metric.getType());
                default:
                    break;
            }
        }
        return timeSeriesMap.values().stream().map(PromTimeSeries::build).toList();
    }

    private void addLongGauge(MetricData metricData) {
        String baseName = PromUtils.normalizeMetricName(metricData);
        for (LongPointData data : metricData.getLongGaugeData().getPoints()) {
            PromLabels labels = PromLabels.fromOTLPMetric(baseName, metricData, data.getAttributes());
            addSample(labels, data.getValue(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));
        }
    }

    private void addDoubleGauge(MetricData metricData) {
        String baseName = PromUtils.normalizeMetricName(metricData);
        for (DoublePointData data : metricData.getDoubleGaugeData().getPoints()) {
            PromLabels labels = PromLabels.fromOTLPMetric(baseName, metricData, data.getAttributes());
            addSample(labels, data.getValue(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));
        }
    }

    private void addLongSum(MetricData metricData) {
        String baseName = PromUtils.normalizeMetricName(metricData);
        for (LongPointData data : metricData.getLongSumData().getPoints()) {
            PromLabels labels = PromLabels.fromOTLPMetric(baseName, metricData, data.getAttributes());
            addSample(labels, data.getValue(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));
        }
    }

    private void addDoubleSum(MetricData metricData) {
        String baseName = PromUtils.normalizeMetricName(metricData);
        for (DoublePointData data : metricData.getDoubleSumData().getPoints()) {
            PromLabels labels = PromLabels.fromOTLPMetric(baseName, metricData, data.getAttributes());
            addSample(labels, data.getValue(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));
        }
    }

    private void addSummary(MetricData metricData) {
        String baseName = PromUtils.normalizeMetricName(metricData);
        for (SummaryPointData data : metricData.getSummaryData().getPoints()) {
            // add sum metric
            PromLabels labels = PromLabels.fromOTLPMetric(baseName + PromConsts.METRIC_NAME_SUFFIX_SUM, metricData, data.getAttributes());
            addSample(labels, data.getSum(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));

            // add count metric
            labels = PromLabels.fromOTLPMetric(baseName + PromConsts.METRIC_NAME_SUFFIX_COUNT, metricData, data.getAttributes());
            addSample(labels, data.getCount(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));

            // add quantile metrics
            for (ValueAtQuantile quantileData : data.getValues()) {
                labels = PromLabels.fromOTLPMetric(baseName, metricData, data.getAttributes(),
                    Map.of(PromConsts.LABEL_NAME_QUANTILE, Double.toString(quantileData.getQuantile())));
                addSample(labels, data.getSum(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));
            }
        }
    }

    public void addHistogram(MetricData metricData) {
        String baseName = PromUtils.normalizeMetricName(metricData);
        for (HistogramPointData data : metricData.getHistogramData().getPoints()) {
            // add sum metric
            PromLabels labels = PromLabels.fromOTLPMetric(baseName + PromConsts.METRIC_NAME_SUFFIX_SUM, metricData, data.getAttributes());
            addSample(labels, data.getSum(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));

            // add count metric
            labels = PromLabels.fromOTLPMetric(baseName + PromConsts.METRIC_NAME_SUFFIX_COUNT, metricData, data.getAttributes());
            addSample(labels, data.getCount(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));

            // add bucket metrics
            for (int i = 0; i < data.getBoundaries().size() && i < data.getCounts().size(); i++) {
                labels = PromLabels.fromOTLPMetric(baseName + PromConsts.METRIC_NAME_SUFFIX_BUCKET, metricData, data.getAttributes(),
                    Map.of(PromConsts.LABEL_NAME_LE, Double.toString(data.getBoundaries().get(i))));
                addSample(labels, data.getCounts().get(i), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));
            }
            labels = PromLabels.fromOTLPMetric(baseName + PromConsts.METRIC_NAME_SUFFIX_BUCKET, metricData, data.getAttributes(),
                Map.of(PromConsts.LABEL_NAME_LE, PromConsts.LABEL_VALUE_INF));
            addSample(labels, data.getCount(), TimeUnit.NANOSECONDS.toMillis(data.getEpochNanos()));
        }
    }

    private void addSample(PromLabels labels, double value, long timestampMillis) {
        PromTimeSeries timeSeries = timeSeriesMap.computeIfAbsent(labels, k -> new PromTimeSeries(labels.toLabels()));
        timeSeries.addSample(RemoteWrite.Sample.newBuilder()
            .setValue(value)
            .setTimestamp(timestampMillis)
            .build());
    }
}
