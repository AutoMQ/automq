package com.automq.stream.s3.metrics;

import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Queue;
import java.util.function.Consumer;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class MetricsHistogramTest {
    /** Verifies HistogramBundle count and sum gauges expose cumulative values across interval snapshots. */
    @Test
    void histogramBundleShouldReportCumulativeCountAndSum() throws Exception {
        Metrics metrics = Metrics.instance();
        Meter previousMeter = getField(metrics, "meter", Meter.class);
        MetricsConfig previousConfig = getField(metrics, "globalConfig", MetricsConfig.class);

        setField(metrics, "meter", null);
        setField(metrics, "globalConfig", null);
        Metrics.HistogramBundle bundle = metrics.histogram("test_histogram", "test", "nanoseconds");
        DeltaHistogram histogram = bundle.histogram(MetricsLevel.INFO, Attributes.empty());

        Meter meter = mock(Meter.class);
        DoubleGaugeBuilder doubleGaugeBuilder = mock(DoubleGaugeBuilder.class);
        LongGaugeBuilder longGaugeBuilder = mock(LongGaugeBuilder.class);
        ObservableDoubleGauge doubleGauge = mock(ObservableDoubleGauge.class);
        ObservableLongGauge longGauge = mock(ObservableLongGauge.class);
        ObservableLongMeasurement countMeasurement = mock(ObservableLongMeasurement.class);
        ObservableLongMeasurement sumMeasurement = mock(ObservableLongMeasurement.class);

        try {
            when(meter.gaugeBuilder(anyString())).thenReturn(doubleGaugeBuilder);
            when(doubleGaugeBuilder.setDescription(anyString())).thenReturn(doubleGaugeBuilder);
            when(doubleGaugeBuilder.setUnit(anyString())).thenReturn(doubleGaugeBuilder);
            when(doubleGaugeBuilder.ofLongs()).thenReturn(longGaugeBuilder);
            when(doubleGaugeBuilder.buildWithCallback(any())).thenReturn(doubleGauge);
            when(longGaugeBuilder.setDescription(anyString())).thenReturn(longGaugeBuilder);
            when(longGaugeBuilder.setUnit(anyString())).thenReturn(longGaugeBuilder);
            when(longGaugeBuilder.buildWithCallback(any())).thenReturn(longGauge);
            setField(metrics, "meter", meter);
            setField(metrics, "globalConfig", new MetricsConfig(MetricsLevel.INFO, Attributes.empty(), 0));

            bundle.setup();
            @SuppressWarnings("unchecked")
            Consumer<ObservableLongMeasurement> countCallback = (Consumer<ObservableLongMeasurement>) getInvocationArgument(
                longGaugeBuilder, 0);
            @SuppressWarnings("unchecked")
            Consumer<ObservableLongMeasurement> sumCallback = (Consumer<ObservableLongMeasurement>) getInvocationArgument(
                longGaugeBuilder, 1);

            histogram.record(10);
            countCallback.accept(countMeasurement);
            sumCallback.accept(sumMeasurement);
            Thread.sleep(2);
            histogram.record(20);
            countCallback.accept(countMeasurement);
            sumCallback.accept(sumMeasurement);

            verify(countMeasurement).record(1L, Attributes.empty());
            verify(countMeasurement).record(2L, Attributes.empty());
            verify(sumMeasurement).record(10L, Attributes.empty());
            verify(sumMeasurement).record(30L, Attributes.empty());
        } finally {
            getWaitingSetups(metrics).remove(bundle);
            setField(metrics, "meter", previousMeter);
            setField(metrics, "globalConfig", previousConfig);
        }
    }

    private static Object getInvocationArgument(LongGaugeBuilder builder, int invocationIndex) {
        return org.mockito.Mockito.mockingDetails(builder)
            .getInvocations()
            .stream()
            .filter(invocation -> invocation.getMethod().getName().equals("buildWithCallback"))
            .skip(invocationIndex)
            .findFirst()
            .orElseThrow()
            .getArgument(0);
    }

    @SuppressWarnings("unchecked")
    private static Queue<Object> getWaitingSetups(Metrics metrics) throws Exception {
        return getField(metrics, "waitingSetups", Queue.class);
    }

    private static <T> T getField(Object target, String name, Class<T> fieldType) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return fieldType.cast(field.get(target));
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
