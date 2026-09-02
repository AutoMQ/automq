package com.automq.stream.s3.metrics;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class MetricsGaugeTest {
    /** Verifies LongGauge can record values lazily from a supplier during callback collection. */
    @Test
    void longGaugeShouldRecordFromSupplierDuringCallback() throws Exception {
        Metrics.LongGaugeBundle.LongGauge gauge = Metrics.instance()
            .longGauge("test_long_supplier", "test", "")
            .register(MetricsLevel.INFO, Attributes.empty());
        AtomicLong value = new AtomicLong(10);
        ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);

        gauge.record(value::get);
        invokeRecord(gauge, ObservableLongMeasurement.class, measurement);
        value.set(20);
        invokeRecord(gauge, ObservableLongMeasurement.class, measurement);

        verify(measurement).record(eq(10L), any());
        verify(measurement).record(eq(20L), any());
        gauge.close();
    }

    /** Verifies DoubleGauge supplier mode is cleared and does not leak stale values. */
    @Test
    void doubleGaugeShouldClearSupplierMode() throws Exception {
        Metrics.DoubleGaugeBundle.DoubleGauge gauge = Metrics.instance()
            .doubleGauge("test_double_supplier", "test", "")
            .register(MetricsLevel.INFO, Attributes.empty());
        ObservableDoubleMeasurement measurement = mock(ObservableDoubleMeasurement.class);

        gauge.record(() -> 0.5);
        invokeRecord(gauge, ObservableDoubleMeasurement.class, measurement);
        gauge.clear();
        invokeRecord(gauge, ObservableDoubleMeasurement.class, measurement);

        verify(measurement).record(eq(0.5), any());
        verify(measurement, never()).record(eq(0.0), any());
        gauge.close();
    }

    /** Verifies LongGauge callback registration can decide whether to emit each callback sample. */
    @Test
    void longGaugeShouldRecordFromCallbackDuringCollection() throws Exception {
        AtomicReference<Boolean> active = new AtomicReference<>(false);
        Metrics.LongGaugeBundle.LongGauge gauge = Metrics.instance()
            .longGauge("test_long_callback", "test", "")
            .register(MetricsLevel.INFO, Attributes.empty(), measurement -> {
                if (active.get()) {
                    measurement.record(10L, Attributes.empty());
                }
            });
        ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);

        invokeRecord(gauge, ObservableLongMeasurement.class, measurement);
        active.set(true);
        invokeRecord(gauge, ObservableLongMeasurement.class, measurement);

        verify(measurement).record(eq(10L), any());
        gauge.close();
    }

    /** Verifies DoubleGauge callback registration can emit dynamic callback samples. */
    @Test
    void doubleGaugeShouldRecordFromCallbackDuringCollection() throws Exception {
        Metrics.DoubleGaugeBundle.DoubleGauge gauge = Metrics.instance()
            .doubleGauge("test_double_callback", "test", "")
            .register(MetricsLevel.INFO, Attributes.empty(),
                measurement -> measurement.record(0.75, Attributes.empty()));
        ObservableDoubleMeasurement measurement = mock(ObservableDoubleMeasurement.class);

        invokeRecord(gauge, ObservableDoubleMeasurement.class, measurement);

        verify(measurement).record(eq(0.75), any());
        gauge.close();
    }

    private static void invokeRecord(Object gauge, Class<?> measurementClass, Object measurement) throws Exception {
        Method record = gauge.getClass().getDeclaredMethod("record", measurementClass);
        record.setAccessible(true);
        record.invoke(gauge, measurement);
    }
}
