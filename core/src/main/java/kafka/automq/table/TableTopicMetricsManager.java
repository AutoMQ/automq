package kafka.automq.table;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class TableTopicMetricsManager {
    private static final Cache<String, Attributes> TOPIC_ATTRIBUTE_CACHE = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(1)).build();
    private static Supplier<Map<String, Long>> delaySupplier = Collections::emptyMap;
    private static Supplier<Map<String, Double>> fieldsPerSecondSupplier = Collections::emptyMap;
    private static ObservableLongGauge delay;
    private static ObservableDoubleGauge fieldsPerSecond;

    public static void initMetrics(Meter meter) {
        String prefix = "kafka_tabletopic_";
        delay = meter.gaugeBuilder(prefix + "delay").ofLongs().setUnit("ms")
            .buildWithCallback(recorder ->
                delaySupplier.get().forEach((topic, delay) -> {
                    if (delay >= 0) {
                        recorder.record(delay, getTopicAttribute(topic));
                    }
                }));
        fieldsPerSecond = meter.gaugeBuilder(prefix + "fps")
            .buildWithCallback(recorder ->
                fieldsPerSecondSupplier.get().forEach((topic, fps) -> recorder.record(fps, getTopicAttribute(topic))));
    }

    public static void setDelaySupplier(Supplier<Map<String, Long>> supplier) {
        delaySupplier = supplier;
    }

    public static void setFieldsPerSecondSupplier(Supplier<Map<String, Double>> supplier) {
        fieldsPerSecondSupplier = supplier;
    }

    private static Attributes getTopicAttribute(String topic) {
        try {
            return TOPIC_ATTRIBUTE_CACHE.get(topic, () -> Attributes.of(AttributeKey.stringKey("topic"), topic));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
