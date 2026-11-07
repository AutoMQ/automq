package com.automq.stream.s3.metrics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@Tag("S3Unit")
class MetricsCounterTest {
    /** Verifies LongCounterBundle exposes a minimal add-only API without per-add callbacks. */
    @Test
    void longCounterBundleShouldExposeAddOnlyCounter() {
        Metrics.LongCounterBundle.LongCounter counter = Metrics.instance()
            .longCounter("test_long_counter", "test", "count")
            .register(MetricsLevel.INFO, Attributes.empty());

        counter.add(1);

        boolean hasCallbackAdd = Arrays.stream(counter.getClass().getDeclaredMethods())
            .anyMatch(method -> method.getName().equals("add")
                && method.getParameterCount() == 2
                && method.getParameterTypes()[0] == long.class
                && method.getParameterTypes()[1] == Consumer.class);
        assertFalse(hasCallbackAdd);
    }

    /** Verifies LongCounterBundle applies base attributes and metrics level before adding values. */
    @Test
    void longCounterBundleShouldApplyConfigWhenAdding() throws Exception {
        Metrics metrics = Metrics.instance();
        Metrics.LongCounterBundle counterBundle = metrics.longCounter("test_configured_counter", "test", "count");
        Metrics.LongCounterBundle.LongCounter infoCounter = counterBundle
            .register(MetricsLevel.INFO, Attributes.builder().put("extra", "v").build());
        Metrics.LongCounterBundle.LongCounter debugCounter = counterBundle
            .register(MetricsLevel.DEBUG, Attributes.empty());
        io.opentelemetry.api.metrics.LongCounter instrument = mock(io.opentelemetry.api.metrics.LongCounter.class);
        Meter previousMeter = getField(metrics, "meter", Meter.class);
        MetricsConfig previousConfig = getField(metrics, "globalConfig", MetricsConfig.class);

        try {
            setField(metrics, "meter", mock(Meter.class));
            setField(metrics, "globalConfig", new MetricsConfig(
                MetricsLevel.INFO, Attributes.builder().put("base", "v2").build()));
            setField(counterBundle, "instrument", instrument);
            invokeSetup(infoCounter);
            invokeSetup(debugCounter);

            assertTrue(infoCounter.add(10));
            assertFalse(debugCounter.add(20));

            verify(instrument).add(10L, Attributes.builder().put("base", "v2").put("extra", "v").build());
        } finally {
            infoCounter.close();
            debugCounter.close();
            getWaitingSetups(metrics).remove(counterBundle);
            setField(metrics, "meter", previousMeter);
            setField(metrics, "globalConfig", previousConfig);
        }
    }

    /** Verifies LongCounterBundle uses counter attributes when metrics is not configured yet. */
    @Test
    void longCounterBundleShouldAddBeforeMetricsSetup() throws Exception {
        Metrics metrics = Metrics.instance();
        Metrics.LongCounterBundle counterBundle = metrics.longCounter("test_unconfigured_counter", "test", "count");
        Attributes attributes = Attributes.builder().put("extra", "v").build();
        Metrics.LongCounterBundle.LongCounter counter = counterBundle.register(MetricsLevel.INFO, attributes);
        io.opentelemetry.api.metrics.LongCounter instrument = mock(io.opentelemetry.api.metrics.LongCounter.class);

        try {
            setField(counterBundle, "instrument", instrument);

            assertTrue(counter.add(10));

            verify(instrument).add(10L, attributes);
        } finally {
            counter.close();
            getWaitingSetups(metrics).remove(counterBundle);
        }
    }

    /** Verifies LongCounterBundle skips filtered counters without touching the OTel instrument. */
    @Test
    void longCounterBundleShouldSkipFilteredCounters() throws Exception {
        Metrics metrics = Metrics.instance();
        Metrics.LongCounterBundle counterBundle = metrics.longCounter("test_filtered_counter", "test", "count");
        Metrics.LongCounterBundle.LongCounter counter = counterBundle.register(MetricsLevel.DEBUG, Attributes.empty());
        io.opentelemetry.api.metrics.LongCounter instrument = mock(io.opentelemetry.api.metrics.LongCounter.class);
        Meter previousMeter = getField(metrics, "meter", Meter.class);
        MetricsConfig previousConfig = getField(metrics, "globalConfig", MetricsConfig.class);

        try {
            setField(metrics, "meter", mock(Meter.class));
            setField(metrics, "globalConfig", new MetricsConfig(MetricsLevel.INFO, Attributes.empty()));
            setField(counterBundle, "instrument", instrument);
            invokeSetup(counter);

            assertFalse(counter.add(10));

            verifyNoInteractions(instrument);
        } finally {
            counter.close();
            getWaitingSetups(metrics).remove(counterBundle);
            setField(metrics, "meter", previousMeter);
            setField(metrics, "globalConfig", previousConfig);
        }
    }

    /** Verifies shared stats counters use LongCounterBundle's add-only API. */
    @Test
    void sharedStatsCountersShouldUseLongCounterBundle() {
        S3ObjectMetrics.recordObject();
    }

    /** Verifies ObservableLongCounterBundle can emit dynamic cumulative samples during collection. */
    @Test
    void observableLongCounterBundleShouldRecordFromCallbackDuringCollection() throws Exception {
        AtomicReference<Boolean> active = new AtomicReference<>(false);
        Metrics.ObservableLongCounterBundle.ObservableLongCounter counter = Metrics.instance()
            .observableLongCounter("test_observable_counter", "test", "count")
            .register(MetricsLevel.INFO, Attributes.builder().put("base", "v1").build(), measurement -> {
                if (active.get()) {
                    measurement.record(10L, Attributes.builder().put("dynamic", "v2").build());
                }
            });
        ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);

        invokeRecord(counter, measurement);
        active.set(true);
        invokeRecord(counter, measurement);

        verify(measurement).record(10L, Attributes.builder().put("base", "v1").put("dynamic", "v2").build());
        counter.close();
    }

    /** Verifies ObservableLongCounterBundle skips filtered callbacks and closed registrations. */
    @Test
    void observableLongCounterBundleShouldSkipFilteredAndClosedCallbacks() throws Exception {
        Metrics metrics = Metrics.instance();
        Metrics.ObservableLongCounterBundle counterBundle = metrics
            .observableLongCounter("test_filtered_observable_counter", "test", "count");
        Metrics.ObservableLongCounterBundle.ObservableLongCounter counter = counterBundle
            .register(MetricsLevel.DEBUG, Attributes.empty(), measurement -> measurement.record(10L));
        ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
        Meter previousMeter = getField(metrics, "meter", Meter.class);
        MetricsConfig previousConfig = getField(metrics, "globalConfig", MetricsConfig.class);

        try {
            setField(metrics, "meter", mock(Meter.class));
            setField(metrics, "globalConfig", new MetricsConfig(MetricsLevel.INFO, Attributes.empty()));
            invokeSetup(counter);

            invokeRecord(counter, measurement);
            counter.close();
            setField(metrics, "globalConfig", new MetricsConfig(MetricsLevel.DEBUG, Attributes.empty()));
            invokeSetup(counter);
            invokeRecord(counter, measurement);

            verify(measurement, never()).record(10L, Attributes.empty());
        } finally {
            counter.close();
            getWaitingSetups(metrics).remove(counterBundle);
            setField(metrics, "meter", previousMeter);
            setField(metrics, "globalConfig", previousConfig);
        }
    }

    @SuppressWarnings("unchecked")
    private static Queue<Object> getWaitingSetups(Metrics metrics) throws Exception {
        return getField(metrics, "waitingSetups", Queue.class);
    }

    private static void invokeSetup(Object counter) throws Exception {
        Method setup = counter.getClass().getDeclaredMethod("setup");
        setup.setAccessible(true);
        setup.invoke(counter);
    }

    private static void invokeRecord(Object counter, ObservableLongMeasurement measurement) throws Exception {
        Method record = counter.getClass().getDeclaredMethod("record", ObservableLongMeasurement.class);
        record.setAccessible(true);
        record.invoke(counter, measurement);
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
