package kafka.log.stream.s3.telemetry.exporter;

import kafka.automq.telemetry.proto.common.v1.AnyValue;
import kafka.automq.telemetry.proto.common.v1.ArrayValue;
import kafka.automq.telemetry.proto.common.v1.InstrumentationScope;
import kafka.automq.telemetry.proto.common.v1.KeyValue;
import kafka.automq.telemetry.proto.metrics.v1.AggregationTemporality;
import kafka.automq.telemetry.proto.metrics.v1.Exemplar;
import kafka.automq.telemetry.proto.metrics.v1.ExponentialHistogram;
import kafka.automq.telemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import kafka.automq.telemetry.proto.metrics.v1.Gauge;
import kafka.automq.telemetry.proto.metrics.v1.Histogram;
import kafka.automq.telemetry.proto.metrics.v1.HistogramDataPoint;
import kafka.automq.telemetry.proto.metrics.v1.Metric;
import kafka.automq.telemetry.proto.metrics.v1.NumberDataPoint;
import kafka.automq.telemetry.proto.metrics.v1.ResourceMetrics;
import kafka.automq.telemetry.proto.metrics.v1.ScopeMetrics;
import kafka.automq.telemetry.proto.metrics.v1.Sum;
import kafka.automq.telemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.ExemplarData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramBuckets;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongExemplarData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.data.SumData;

/**
 * OpenTelemetry metric data converter in OTLP Protobuf format.
 *
 * <p>This utility class is used to convert {@link MetricData} objects into
 * {@link org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.ResourceMetrics} objects
 * that comply with the <a href="https://github.com/open-telemetry/opentelemetry-proto">OTLP/Protobuf</a> specification.
 * It is suitable for scenarios where metric data needs to be directly serialized and sent.
 *
 * <h2>Main Features</h2>
 * <ul>
 *   <li>Supports all OpenTelemetry metric types (Gauge/Sum/Histogram/ExponentialHistogram)</li>
 *   <li>Automatically handles differences between Long and Double type data points</li>
 *   <li>Correctly maps metadata such as resources, metrics, and instrumentation scope information</li>
 *   <li>Is compatible with the OpenTelemetry Java SDK version 1.32.0 and above</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Convert MetricData
 * MetricProtoConverter converter = new MetricProtoConverter();
 * ResourceMetrics protoMetrics = converter.convertToResourceMetrics(metricData);
 *
 * // Serialize to a byte stream
 * byte[] bytes = protoMetrics.toByteArray();
 * }
 * </pre>
 *
 * <h2>Version Compatibility</h2>
 * <ul>
 *   <li><strong>OpenTelemetry SDK</strong>: Requires version {@code >= 1.32.0} (due to metric API refactoring)</li>
 *   <li><strong>Protobuf Dependency</strong>: Uses {@code opentelemetry-proto 1.4.0-alpha}</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * This class has no internal state, and all methods are pure functions without side effects.
 * <strong>It can be called thread-safely</strong>.
 *
 * <h2>Error Handling</h2>
 * When an unsupported metric type is encountered, an {@link IllegalArgumentException} will be thrown.
 * The caller needs to catch and handle it.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otel/protocol/">OTLP Protocol Specification</a>
 * @see org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.ResourceMetrics Protobuf data structure
 */
public class MetricProtoConverter {

    public ResourceMetrics convertSingleResourceMetrics(List<MetricData> metrics) {
        if (metrics.isEmpty()) {
            return ResourceMetrics.getDefaultInstance();
        }

        // Assume that all MetricData in the same batch belong to the same Resource
        io.opentelemetry.sdk.resources.Resource resource = metrics.get(0).getResource();
        ResourceMetrics.Builder builder = ResourceMetrics.newBuilder()
                .setResource(convertResource(resource));

        // Group by InstrumentationScope
        Map<InstrumentationScopeInfo, List<MetricData>> grouped = metrics.stream()
                .collect(Collectors.groupingBy(MetricData::getInstrumentationScopeInfo));

        for (Map.Entry<InstrumentationScopeInfo, List<MetricData>> entry : grouped.entrySet()) {
            ScopeMetrics.Builder scopeMetricsBuilder = ScopeMetrics.newBuilder()
                    .setScope(convertInstrumentationScope(entry.getKey()));

            for (MetricData metric : entry.getValue()) {
                scopeMetricsBuilder.addMetrics(convertMetric(metric));
            }
            builder.addScopeMetrics(scopeMetricsBuilder.build());
        }
        return builder.build();
    }

    /**
     * Use the Public API: Single MetricData → ResourceMetrics
     * @param metricData A single MetricData
     * @return ResourceMetrics
     */
    public ResourceMetrics convertToResourceMetrics(MetricData metricData) {
        return ResourceMetrics.newBuilder()
                .setResource(convertResource(metricData.getResource()))
                .addScopeMetrics(convertScopeMetrics(metricData))
                .build();
    }

    private Resource convertResource(io.opentelemetry.sdk.resources.Resource sdkResource) {
        Resource.Builder builder = Resource.newBuilder();
        sdkResource.getAttributes().forEach((key, value) ->
                builder.addAttributes(KeyValue.newBuilder()
                        .setKey(key.getKey())
                        .setValue(convertAnyValue(value))
                        .build())
        );
        return builder.build();
    }

    private ScopeMetrics convertScopeMetrics(MetricData metricData) {
        return ScopeMetrics.newBuilder()
                .setScope(convertInstrumentationScope(metricData.getInstrumentationScopeInfo()))
                .addMetrics(convertMetric(metricData))
                .build();
    }

    private InstrumentationScope convertInstrumentationScope(InstrumentationScopeInfo scope) {
        return InstrumentationScope.newBuilder()
                .setName(scope.getName() == null ? "" : scope.getName())
                .setVersion(scope.getVersion() == null ? "" : scope.getVersion())
                .build();
    }

    private Metric convertMetric(MetricData metricData) {
        Metric.Builder builder = Metric.newBuilder()
                .setName(metricData.getName())
                .setDescription(metricData.getDescription())
                .setUnit(metricData.getUnit());

        switch (metricData.getType()) {
            case LONG_GAUGE:
            case DOUBLE_GAUGE:
                builder.setGauge(convertGauge((GaugeData<?>) metricData.getData()));
                break;
            case LONG_SUM:
            case DOUBLE_SUM:
                builder.setSum(convertSum((SumData<?>) metricData.getData()));
                break;
            case HISTOGRAM:
                builder.setHistogram(convertHistogram((HistogramData) metricData.getData()));
                break;
            case EXPONENTIAL_HISTOGRAM:
                builder.setExponentialHistogram(convertExponentialHistogram((ExponentialHistogramData) metricData.getData()));
                break;
            default:
                throw new IllegalArgumentException("Unsupported metric type: " + metricData.getType());
        }

        return builder.build();
    }

    private Sum convertSum(SumData<? extends PointData> sumData) {
        Sum.Builder sumBuilder = Sum.newBuilder()
                .setAggregationTemporality(convertTemporality(sumData.getAggregationTemporality()))
                .setIsMonotonic(sumData.isMonotonic());

        for (PointData point : sumData.getPoints()) {
            sumBuilder.addDataPoints(convertNumberPoint(point));
        }

        return sumBuilder.build();
    }

    private Gauge convertGauge(GaugeData<? extends PointData> gaugeData) {
        Gauge.Builder gaugeBuilder = Gauge.newBuilder();
        for (PointData point : gaugeData.getPoints()) {
            NumberDataPoint protoPoint = convertNumberPoint(point);
            gaugeBuilder.addDataPoints(protoPoint);
        }
        return gaugeBuilder.build();
    }

    private NumberDataPoint convertNumberPoint(PointData point) {
        NumberDataPoint.Builder protoPoint = NumberDataPoint.newBuilder()
                .setStartTimeUnixNano(point.getStartEpochNanos())
                .setTimeUnixNano(point.getEpochNanos())
                .addAllAttributes(convertAttributes(point.getAttributes()))
                .addAllExemplars(convertExemplars(point.getExemplars(), point instanceof LongPointData));

        // Handle values based on type (compatible with old and new versions)
        if (point instanceof LongPointData) {
            protoPoint.setAsInt(((LongPointData) point).getValue());
        } else if (point instanceof DoublePointData) {
            protoPoint.setAsDouble(((DoublePointData) point).getValue());
        } else {
            throw new IllegalArgumentException("Unsupported point type: " + point.getClass());
        }

        return protoPoint.build();
    }

    private Histogram convertHistogram(HistogramData histogramData) {
        Histogram.Builder builder = Histogram.newBuilder()
                .setAggregationTemporality(convertTemporality(histogramData.getAggregationTemporality()));

        for (HistogramPointData point : histogramData.getPoints()) {
            builder.addDataPoints(convertHistogramPoint(point));
        }
        return builder.build();
    }

    private HistogramDataPoint convertHistogramPoint(HistogramPointData point) {
        HistogramDataPoint.Builder builder = HistogramDataPoint.newBuilder()
                .setStartTimeUnixNano(point.getStartEpochNanos())
                .setTimeUnixNano(point.getEpochNanos())
                .setCount(point.getCount())
                .setSum(point.getSum())
                .addAllExplicitBounds(point.getBoundaries())
                .addAllBucketCounts(point.getCounts())
                .addAllAttributes(convertAttributes(point.getAttributes()))
                .addAllExemplars(convertExemplars(point.getExemplars(), false));

        if (point.hasMin()) {
            builder.setMin(point.getMin());
        }
        if (point.hasMax()) {
            builder.setMax(point.getMax());
        }
        return builder.build();
    }

    private ExponentialHistogram convertExponentialHistogram(ExponentialHistogramData histogramData) {
        ExponentialHistogram.Builder builder = ExponentialHistogram.newBuilder()
                .setAggregationTemporality(convertTemporality(histogramData.getAggregationTemporality()));

        for (ExponentialHistogramPointData point : histogramData.getPoints()) {
            builder.addDataPoints(convertExponentialHistogramPoint(point));
        }
        return builder.build();
    }

    private ExponentialHistogramDataPoint convertExponentialHistogramPoint(ExponentialHistogramPointData point) {
        ExponentialHistogramDataPoint.Builder builder = ExponentialHistogramDataPoint.newBuilder()
                .setStartTimeUnixNano(point.getStartEpochNanos())
                .setTimeUnixNano(point.getEpochNanos())
                .setScale(point.getScale())
                .setSum(point.getSum())
                .setZeroCount(point.getZeroCount())
                .setCount(point.getCount())
                .addAllAttributes(convertAttributes(point.getAttributes()))
                .setPositive(convertBuckets(point.getPositiveBuckets()))
                .setNegative(convertBuckets(point.getNegativeBuckets()));

        if (point.hasMin()) {
            builder.setMin(point.getMin());
        }
        if (point.hasMax()) {
            builder.setMax(point.getMax());
        }
        return builder.build();
    }

    private ExponentialHistogramDataPoint.Buckets convertBuckets(ExponentialHistogramBuckets buckets) {
        return ExponentialHistogramDataPoint.Buckets.newBuilder()
                .setOffset(buckets.getOffset())
                .addAllBucketCounts(buckets.getBucketCounts())
                .build();
    }

    private AggregationTemporality convertTemporality(io.opentelemetry.sdk.metrics.data.AggregationTemporality temporality) {
        switch (temporality) {
            case CUMULATIVE:
                return AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
            case DELTA:
                return AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
            default:
                return AggregationTemporality.AGGREGATION_TEMPORALITY_UNSPECIFIED;
        }
    }

    /**
     * Determine the encoding method of Exemplar.Value based on the data type (Long or Double) of the parent metric point.
     * - LongPointData (LONG_GAUGE / LONG_SUM) → as_int
     * - DoublePointData (other types) → as_double
     */
    private List<Exemplar> convertExemplars(List<? extends ExemplarData> exemplars, boolean isParentLongType) {
        List<Exemplar> protoExemplars = new ArrayList<>();
        for (ExemplarData exemplar : exemplars) {
            Exemplar.Builder protoExemplar = Exemplar.newBuilder()
                    .setTimeUnixNano(exemplar.getEpochNanos())
                    .addAllFilteredAttributes(convertAttributes(exemplar.getFilteredAttributes()));

            if (isParentLongType) {
                if (exemplar instanceof LongExemplarData) {
                    long value = ((LongExemplarData) exemplar).getValue();
                    protoExemplar.setAsInt(value);
                }
                // The SDK ensures that the Exemplar.Value from a LongPoint has been safely converted to a double without loss of precision.
            } else {
                if (exemplar instanceof DoubleExemplarData) {
                    double value = ((DoubleExemplarData) exemplar).getValue();
                    protoExemplar.setAsDouble(value);
                }
            }

            // Add SpanContext (if it exists)
            if (exemplar.getSpanContext() != null && exemplar.getSpanContext().isValid()) {
                protoExemplar.setSpanId(ByteString.copyFrom(exemplar.getSpanContext().getSpanIdBytes()));
                protoExemplar.setTraceId(ByteString.copyFrom(exemplar.getSpanContext().getTraceIdBytes()));
            }

            protoExemplars.add(protoExemplar.build());
        }
        return protoExemplars;
    }

    private List<KeyValue> convertAttributes(Attributes attributes) {
        List<KeyValue> keyValues = new ArrayList<>();
        attributes.forEach((key, value) ->
                keyValues.add(KeyValue.newBuilder()
                        .setKey(key.getKey())
                        .setValue(convertAnyValue(value))
                        .build())
        );
        return keyValues;
    }

    private AnyValue convertAnyValue(Object value) {
        if (value == null) {
            return AnyValue.newBuilder().setStringValue("null").build();
        }
        AnyValue.Builder builder = AnyValue.newBuilder();
        if (value instanceof String) {
            builder.setStringValue((String) value);
        } else if (value instanceof Boolean) {
            builder.setBoolValue((Boolean) value);
        } else if (value instanceof Long) {
            builder.setIntValue((Long) value);
        } else if (value instanceof Double) {
            builder.setDoubleValue((Double) value);
        } else if (value instanceof List) {
            // Handle array types
            ArrayValue.Builder arrayBuilder = ArrayValue.newBuilder();
            for (Object element : (List<?>) value) {
                arrayBuilder.addValues(convertAnyValue(element));
            }
            builder.setArrayValue(arrayBuilder.build());
        } else if (value instanceof byte[]) {
            // Handle byte arrays
            builder.setBytesValue(ByteString.copyFrom((byte[]) value));
        } else {
            // Fallback logic
            builder.setStringValue(value.toString());
        }
        return builder.build();
    }
}
