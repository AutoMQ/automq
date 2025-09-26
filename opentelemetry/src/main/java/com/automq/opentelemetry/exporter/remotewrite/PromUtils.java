package com.automq.opentelemetry.exporter.remotewrite;

import com.automq.opentelemetry.exporter.s3.PrometheusUtils;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;

public class PromUtils {

    public static String normalizeMetricName(MetricData metricData) {
        return PrometheusUtils.mapMetricsName(metricData.getName(), metricData.getUnit(), isCounter(metricData), isGauge(metricData));
    }

    private static boolean isCounter(MetricData metricData) {
        if (metricData.getType() == MetricDataType.DOUBLE_SUM) {
            return metricData.getDoubleSumData().isMonotonic();
        }
        if (metricData.getType() == MetricDataType.LONG_SUM) {
            return metricData.getLongSumData().isMonotonic();
        }
        return false;
    }

    private static boolean isGauge(MetricData metricData) {
        return metricData.getType() == MetricDataType.LONG_GAUGE || metricData.getType() == MetricDataType.DOUBLE_GAUGE;
    }

    public static String normalizeLabel(String labelKey) {
        return labelKey.replaceAll("[^a-zA-Z0-9_]", "_");
    }

}
