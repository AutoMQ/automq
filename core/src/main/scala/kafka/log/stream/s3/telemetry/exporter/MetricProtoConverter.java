package kafka.log.stream.s3.telemetry.exporter;

import kafka.automq.telemetry.proto.common.v1.AnyValue;
import kafka.automq.telemetry.proto.common.v1.ArrayValue;
import kafka.automq.telemetry.proto.common.v1.InstrumentationScope;
import kafka.automq.telemetry.proto.common.v1.KeyValue;
import kafka.automq.telemetry.proto.metrics.v1.*;
import kafka.automq.telemetry.proto.metrics.v1.AggregationTemporality;
import kafka.automq.telemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.*;

/**
 * OTLP Protobuf 格式的 OpenTelemetry 指标数据转换器
 *
 * <p>该工具类用于将 {@link MetricData} 对象转换为符合
 * <a href="https://github.com/open-telemetry/opentelemetry-proto">OTLP/Protobuf</a> 规范的
 * {@link org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.ResourceMetrics} 对象，适用于需直接序列化/发送指标数据的场景。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>支持全部 OpenTelemetry 指标类型（Gauge/Sum/Histogram/ExponentialHistogram）</li>
 *   <li>自动处理 Long/Double 类型数据点的差异</li>
 *   <li>正确映射资源（Resource）、监控项（Metric）、范围信息（Instrumentation Scope）等元数据</li>
 *   <li>适配 OpenTelemetry Java SDK 1.32.0 及以上版本的 API</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 转换 MetricData
 * MetricProtoConverter converter = new MetricProtoConverter();
 * ResourceMetrics protoMetrics = converter.convertToResourceMetrics(metricData);
 *
 * // 序列化为字节流
 * byte[] bytes = protoMetrics.toByteArray();
 * }
 * </pre>
 *
 * <h2>版本兼容性</h2>
 * <ul>
 *   <li><strong>OpenTelemetry SDK</strong>: 要求版本 {@code >= 1.32.0} （因指标 API 重构）</li>
 *   <li><strong>Protobuf 依赖</strong>: 使用 {@code opentelemetry-proto 1.4.0-alpha} </li>
 * </ul>
 *
 * <h2>线程安全</h2>
 * 该类无内部状态，所有方法均为无副作用纯函数，<strong>可线程安全调用</strong>。
 *
 * <h2>错误处理</h2>
 * 当遇到不支持的指标类型时会抛出 {@link IllegalArgumentException}，调用方需捕获处理。
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otel/protocol/">OTLP 协议规范</a>
 * @see org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.ResourceMetrics Protobuf 数据结构
 */
public class MetricProtoConverter {

    public ResourceMetrics convertSingleResourceMetrics(List<MetricData> metrics) {
        if (metrics.isEmpty()) {
            return ResourceMetrics.getDefaultInstance();
        }

        // 假定同一批 MetricData 属于同一 Resource
        io.opentelemetry.sdk.resources.Resource resource = metrics.get(0).getResource();
        ResourceMetrics.Builder builder = ResourceMetrics.newBuilder()
                .setResource(convertResource(resource));

        // 按 InstrumentationScope 分组
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
     * 使用Public API: 单个 MetricData → ResourceMetrics
     * @param metricData 单个 MetricData
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

        // 根据类型处理数值（兼容新旧版本）
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
     *
     * 根据父级指标点的数据类型（Long 或 Double）判断 Exemplar.Value 的编码方式
     * - LongPointData（LONG_GAUGE / LONG_SUM） → as_int
     * - DoublePointData（其余类型） → as_double
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
                // SDK 保证：来自 LongPoint 的 Exemplar.Value 已被安全转换为无精度丢失的 double
            } else {
                if (exemplar instanceof DoubleExemplarData) {
                    double value = ((DoubleExemplarData) exemplar).getValue();
                    protoExemplar.setAsDouble(value);
                }
            }

            // 添加 SpanContext（如果存在）
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
            // 处理数组类型
            ArrayValue.Builder arrayBuilder = ArrayValue.newBuilder();
            for (Object element : (List<?>) value) {
                arrayBuilder.addValues(convertAnyValue(element));
            }
            builder.setArrayValue(arrayBuilder.build());
        } else if (value instanceof byte[]) {
            // 处理字节数组
            builder.setBytesValue(ByteString.copyFrom((byte[]) value));
        } else {
            // Fallback 逻辑
            builder.setStringValue(value.toString());
        }
        return builder.build();
    }
}
