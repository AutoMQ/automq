/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.server.metrics.s3stream;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.NoopLongCounter;
import com.automq.stream.s3.metrics.NoopObservableLongGauge;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramInstrument;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

public class S3StreamKafkaMetricsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamKafkaMetricsManager.class);

    private static final List<ConfigListener> BASE_ATTRIBUTES_LISTENERS = new ArrayList<>();

    public static final List<HistogramMetric> FETCH_LIMITER_TIME_METRICS = new CopyOnWriteArrayList<>();

    private static final MultiAttributes<String> BROKER_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_NODE_ID);
    private static final MultiAttributes<String> S3_OBJECT_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_OBJECT_STATE);
    private static final MultiAttributes<String> FETCH_LIMITER_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_FETCH_LIMITER_NAME);
    private static final MultiAttributes<String> FETCH_EXECUTOR_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_FETCH_EXECUTOR_NAME);
    private static final MultiAttributes<String> PARTITION_STATUS_STATISTICS_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_STATUS);
    private static final MultiAttributes<String> BACK_PRESSURE_STATE_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_BACK_PRESSURE_STATE);

    // List to store all the observable long gauges for certificates
    private static final List<ObservableLongGauge> CERT_OBSERVABLE_LONG_GAUGES = new ArrayList<>();

    static {
        BASE_ATTRIBUTES_LISTENERS.add(BROKER_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(S3_OBJECT_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(FETCH_LIMITER_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(FETCH_EXECUTOR_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(BACK_PRESSURE_STATE_ATTRIBUTES);
    }

    private static Supplier<Boolean> isActiveSupplier = () -> false;
    private static ObservableLongGauge autoBalancerMetricsTimeDelay = new NoopObservableLongGauge();
    private static Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier = Collections::emptyMap;
    private static ObservableLongGauge s3ObjectCountMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<String, Integer>> s3ObjectCountMapSupplier = Collections::emptyMap;
    private static ObservableLongGauge s3ObjectSizeMetrics = new NoopObservableLongGauge();
    private static Supplier<Long> s3ObjectSizeSupplier = () -> 0L;
    private static ObservableLongGauge streamSetObjectNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<String, Integer>> streamSetObjectNumSupplier = Collections::emptyMap;
    private static ObservableLongGauge streamObjectNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Integer> streamObjectNumSupplier = () -> 0;

    private static ObservableLongGauge fetchLimiterPermitNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<String, Integer>> fetchLimiterPermitNumSupplier = Collections::emptyMap;
    private static ObservableLongGauge fetchLimiterWaitingTaskNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<String, Integer>> fetchLimiterWaitingTaskNumSupplier = Collections::emptyMap;
    private static ObservableLongGauge fetchPendingTaskNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<String, Integer>> fetchPendingTaskNumSupplier = Collections::emptyMap;
    private static LongCounter fetchLimiterTimeoutCount = new NoopLongCounter();
    private static HistogramInstrument fetchLimiterTime;

    private static ObservableLongGauge logAppendPermitNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Integer> logAppendPermitNumSupplier = () -> 0;
    private static MetricsConfig metricsConfig = new MetricsConfig(MetricsLevel.INFO, Attributes.empty());
    private static ObservableLongGauge slowBrokerMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<Integer, Boolean>> slowBrokerSupplier = Collections::emptyMap;
    private static ObservableLongGauge topicPartitionCountMetrics = new NoopObservableLongGauge();
    private static Supplier<PartitionCountDistribution> topicPartitionCountSupplier = () -> null;

    private static ObservableLongGauge partitionStatusStatisticsMetrics = new NoopObservableLongGauge();
    private static List<String> partitionStatusList = Collections.emptyList();
    private static Function<String, Integer> partitionStatusStatisticsSupplier = s -> 0;

    private static ObservableLongGauge backPressureState = new NoopObservableLongGauge();
    /**
     * Supplier for back pressure state.
     * Key is the state name, value is 1 for current state, -1 for other states.
     */
    private static Supplier<Map<String, Integer>> backPressureStateSupplier = Collections::emptyMap;

    /**
     * supplier for truststoreCerts
     */
    private static Supplier<String> truststoreCertsSupplier = () -> null;
    /**
     * supplier for server cert chain
     */
    private static Supplier<String> certChainSupplier = () -> null;

    public static void configure(MetricsConfig metricsConfig) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            S3StreamKafkaMetricsManager.metricsConfig = metricsConfig;
            for (ConfigListener listener : BASE_ATTRIBUTES_LISTENERS) {
                listener.onConfigChange(metricsConfig);
            }
        }
    }

    public static void initMetrics(Meter meter, String prefix) {
        initAutoBalancerMetrics(meter, prefix);
        initObjectMetrics(meter, prefix);
        initFetchMetrics(meter, prefix);
        initLogAppendMetrics(meter, prefix);
        initPartitionStatusStatisticsMetrics(meter, prefix);
        initBackPressureMetrics(meter, prefix);
        try {
            initCertMetrics(meter, prefix);
        } catch (Exception e) {
            LOGGER.error("Failed to init cert metrics", e);
        }
    }

    private static void initAutoBalancerMetrics(Meter meter, String prefix) {
        autoBalancerMetricsTimeDelay = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.AUTO_BALANCER_METRICS_TIME_DELAY_METRIC_NAME)
                .setDescription("The time delay of auto balancer metrics per broker")
                .setUnit("ms")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && isActiveSupplier.get()) {
                        Map<Integer, Long> metricsTimeDelayMap = autoBalancerMetricsTimeMapSupplier.get();
                        for (Map.Entry<Integer, Long> entry : metricsTimeDelayMap.entrySet()) {
                            long timestamp = entry.getValue();
                            long delay = timestamp == 0 ? -1 : System.currentTimeMillis() - timestamp;
                            result.record(delay, BROKER_ATTRIBUTES.get(String.valueOf(entry.getKey())));
                        }
                    }
                });
        slowBrokerMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.SLOW_BROKER_METRIC_NAME)
                .setDescription("The metrics to indicate whether the broker is slow or not")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && isActiveSupplier.get()) {
                        Map<Integer, Boolean> slowBrokerMap = slowBrokerSupplier.get();
                        for (Map.Entry<Integer, Boolean> entry : slowBrokerMap.entrySet()) {
                            result.record(entry.getValue() ? 1 : 0, BROKER_ATTRIBUTES.get(String.valueOf(entry.getKey())));
                        }
                    }
                });
        topicPartitionCountMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.TOPIC_PARTITION_COUNT_METRIC_NAME)
                .setDescription("The number of partitions for each topic on each broker")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        PartitionCountDistribution partitionCountDistribution = topicPartitionCountSupplier.get();
                        if (partitionCountDistribution == null) {
                            return;
                        }
                        for (Map.Entry<String, Integer> entry : partitionCountDistribution.topicPartitionCount().entrySet()) {
                            String topic = entry.getKey();
                            result.record(entry.getValue(), Attributes.builder()
                                    .put(S3StreamKafkaMetricsConstants.LABEL_TOPIC_NAME, topic)
                                    .put(S3StreamKafkaMetricsConstants.LABEL_RACK_ID, partitionCountDistribution.rack())
                                    .put(S3StreamKafkaMetricsConstants.LABEL_NODE_ID, String.valueOf(partitionCountDistribution.brokerId()))
                                    .build());
                        }
                    }
                });
    }

    private static void initObjectMetrics(Meter meter, String prefix) {
        s3ObjectCountMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.S3_OBJECT_COUNT_BY_STATE)
                .setDescription("The total count of s3 objects in different states")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && isActiveSupplier.get()) {
                        Map<String, Integer> s3ObjectCountMap = s3ObjectCountMapSupplier.get();
                        for (Map.Entry<String, Integer> entry : s3ObjectCountMap.entrySet()) {
                            result.record(entry.getValue(), S3_OBJECT_ATTRIBUTES.get(entry.getKey()));
                        }
                    }
                });
        s3ObjectSizeMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.S3_OBJECT_SIZE)
                .setDescription("The total size of s3 objects in bytes")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && isActiveSupplier.get()) {
                        result.record(s3ObjectSizeSupplier.get(), metricsConfig.getBaseAttributes());
                    }
                });
        streamSetObjectNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.STREAM_SET_OBJECT_NUM)
                .setDescription("The total number of stream set objects")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && isActiveSupplier.get()) {
                        Map<String, Integer> streamSetObjectNumMap = streamSetObjectNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : streamSetObjectNumMap.entrySet()) {
                            result.record(entry.getValue(), BROKER_ATTRIBUTES.get(entry.getKey()));
                        }
                    }
                });
        streamObjectNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.STREAM_OBJECT_NUM)
                .setDescription("The total number of stream objects")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && isActiveSupplier.get()) {
                        result.record(streamObjectNumSupplier.get(), metricsConfig.getBaseAttributes());
                    }
                });
    }

    private static void initFetchMetrics(Meter meter, String prefix) {
        fetchLimiterPermitNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.FETCH_LIMITER_PERMIT_NUM)
                .setDescription("The number of permits in fetch limiters")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        Map<String, Integer> fetchLimiterPermitNumMap = fetchLimiterPermitNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : fetchLimiterPermitNumMap.entrySet()) {
                            result.record(entry.getValue(), FETCH_LIMITER_ATTRIBUTES.get(entry.getKey()));
                        }
                    }
                });
        fetchLimiterWaitingTaskNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.FETCH_LIMITER_WAITING_TASK_NUM)
                .setDescription("The number of tasks waiting for permits in fetch limiters")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        Map<String, Integer> fetchLimiterWaitingTaskNumMap = fetchLimiterWaitingTaskNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : fetchLimiterWaitingTaskNumMap.entrySet()) {
                            result.record(entry.getValue(), FETCH_LIMITER_ATTRIBUTES.get(entry.getKey()));
                        }
                    }
                });

        fetchPendingTaskNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.FETCH_PENDING_TASK_NUM)
                .setDescription("The number of pending tasks in fetch executors")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        Map<String, Integer> fetchPendingTaskNumMap = fetchPendingTaskNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : fetchPendingTaskNumMap.entrySet()) {
                            result.record(entry.getValue(), FETCH_EXECUTOR_ATTRIBUTES.get(entry.getKey()));
                        }
                    }
                });

        fetchLimiterTimeoutCount = meter.counterBuilder(prefix + S3StreamKafkaMetricsConstants.FETCH_LIMITER_TIMEOUT_COUNT)
                .setDescription("The number of acquire permits timeout in fetch limiters")
                .build();
        fetchLimiterTime = new HistogramInstrument(meter, prefix + S3StreamKafkaMetricsConstants.FETCH_LIMITER_TIME,
                "The time cost of acquire permits in fetch limiters", "nanoseconds", () -> FETCH_LIMITER_TIME_METRICS);
    }

    private static void initLogAppendMetrics(Meter meter, String prefix) {
        logAppendPermitNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.LOG_APPEND_PERMIT_NUM)
                .setDescription("The number of permits in elastic log append limiter")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        result.record(logAppendPermitNumSupplier.get(), metricsConfig.getBaseAttributes());
                    }
                });
    }

    private static void initPartitionStatusStatisticsMetrics(Meter meter, String prefix) {
        partitionStatusStatisticsMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.PARTITION_STATUS_STATISTICS_METRIC_NAME)
                .setDescription("The statistics of partition status")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        for (String partitionStatus : partitionStatusList) {
                            result.record(partitionStatusStatisticsSupplier.apply(partitionStatus), PARTITION_STATUS_STATISTICS_ATTRIBUTES.get(partitionStatus));
                        }
                    }
                });
    }

    private static void initBackPressureMetrics(Meter meter, String prefix) {
        backPressureState = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.BACK_PRESSURE_STATE_METRIC_NAME)
                .setDescription("Back pressure state")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        Map<String, Integer> states = backPressureStateSupplier.get();
                        states.forEach((state, value) -> {
                            result.record(value, BACK_PRESSURE_STATE_ATTRIBUTES.get(state));
                        });
                    }
                });
    }

    /**
     * Initialize the certificate metrics.
     *
     * @param meter The OpenTelemetry meter to use for creating metrics.
     */
    public static void initCertMetrics(Meter meter, String prefix) throws CertificateException {
        String truststoreCerts = truststoreCertsSupplier.get();
        String certChain = certChainSupplier.get();
        if (truststoreCerts == null || truststoreCerts.isEmpty()) {
            return;
        }
        if (certChain == null || certChain.isEmpty()) {
            return;
        }
        // Add TLS certificate metrics
        addTlsMetrics(certChain, truststoreCerts, meter, prefix);
    }

    /**
     * Add TLS certificate metrics.
     *
     * @param certChain       The certificate chain in PEM format.
     * @param truststoreCerts The truststore certificates in PEM format.
     * @param meter           The OpenTelemetry meter to use for creating metrics.
     * @param prefix          The prefix for the metric names.
     */
    private static void addTlsMetrics(String certChain, String truststoreCerts, Meter meter, String prefix) throws CertificateException {
        // Parse and check the certificate expiration time
        X509Certificate[] serverCerts = parseCertificates(certChain);
        X509Certificate[] trustStoreCerts = parseCertificates(truststoreCerts);

        for (X509Certificate cert : serverCerts) {
            registerCertMetrics(meter, cert, "server_cert", prefix);
        }
        for (X509Certificate cert : trustStoreCerts) {
            registerCertMetrics(meter, cert, "truststore_cert", prefix);
        }
    }

    /**
     * Register certificate metrics.
     *
     * @param meter    The OpenTelemetry meter to use for creating metrics.
     * @param cert     The X509 certificate to register metrics for.
     * @param certType The type of the certificate (e.g., "server_cert", "truststore_cert").
     * @param prefix   The prefix for the metric names.
     */
    private static void registerCertMetrics(Meter meter, X509Certificate cert, String certType, String prefix) {
        String subject = cert.getSubjectX500Principal().getName();
        Date expiryDate = cert.getNotAfter();
        long daysRemaining = (expiryDate.getTime() - System.currentTimeMillis()) / (1000 * 3600 * 24);

        // Create and register Gauge metrics
        Attributes attributes = Attributes.builder()
                .put("cert_type", certType)
                .put("cert_subject", subject)
                .build();

        ObservableLongGauge observableCertExpireMills = meter.gaugeBuilder(prefix + "cert_expiry_timestamp")
                .setDescription("The expiry timestamp of the TLS certificate")
                .setUnit("milliseconds")
                .ofLongs()
                .buildWithCallback(result -> result.record(expiryDate.getTime(), attributes));
        CERT_OBSERVABLE_LONG_GAUGES.add(observableCertExpireMills);

        ObservableLongGauge observableCertExpireDays = meter.gaugeBuilder(prefix + "cert_days_remaining")
                .setDescription("The remaining days until the TLS certificate expires")
                .setUnit("days")
                .ofLongs()
                .buildWithCallback(result -> result.record(daysRemaining, attributes));
        CERT_OBSERVABLE_LONG_GAUGES.add(observableCertExpireDays);
    }

    /**
     * Parse the PEM formatted certificate content into an array of X509 certificates.
     *
     * @param pemContent The PEM formatted certificate content.
     * @return An array of X509 certificates.
     * @throws CertificateException If there is an error parsing the certificates.
     */
    private static X509Certificate[] parseCertificates(String pemContent) throws CertificateException {
        String[] pemArray = pemContent.split("-----END CERTIFICATE-----");
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        X509Certificate[] certs = new X509Certificate[pemArray.length];

        for (int i = 0; i < pemArray.length; i++) {
            String pemPart = pemArray[i];
            byte[] certBytes = Base64.getDecoder().decode(pemPart.replace("-----BEGIN CERTIFICATE-----", "").replaceAll("\n", ""));
            certs[i] = (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
        }
        return certs;
    }

    public static void setIsActiveSupplier(Supplier<Boolean> isActiveSupplier) {
        S3StreamKafkaMetricsManager.isActiveSupplier = isActiveSupplier;
    }

    public static void setAutoBalancerMetricsTimeMapSupplier(Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier) {
        S3StreamKafkaMetricsManager.autoBalancerMetricsTimeMapSupplier = autoBalancerMetricsTimeMapSupplier;
    }

    public static void setS3ObjectCountMapSupplier(Supplier<Map<String, Integer>> s3ObjectCountMapSupplier) {
        S3StreamKafkaMetricsManager.s3ObjectCountMapSupplier = s3ObjectCountMapSupplier;
    }

    public static void setS3ObjectSizeSupplier(Supplier<Long> s3ObjectSizeSupplier) {
        S3StreamKafkaMetricsManager.s3ObjectSizeSupplier = s3ObjectSizeSupplier;
    }

    public static void setStreamSetObjectNumSupplier(Supplier<Map<String, Integer>> streamSetObjectNumSupplier) {
        S3StreamKafkaMetricsManager.streamSetObjectNumSupplier = streamSetObjectNumSupplier;
    }

    public static void setStreamObjectNumSupplier(Supplier<Integer> streamObjectNumSupplier) {
        S3StreamKafkaMetricsManager.streamObjectNumSupplier = streamObjectNumSupplier;
    }

    public static void setFetchLimiterPermitNumSupplier(Supplier<Map<String, Integer>> fetchLimiterPermitNumSupplier) {
        S3StreamKafkaMetricsManager.fetchLimiterPermitNumSupplier = fetchLimiterPermitNumSupplier;
    }

    public static void setFetchLimiterWaitingTaskNumSupplier(Supplier<Map<String, Integer>> fetchLimiterWaitingTaskNumSupplier) {
        S3StreamKafkaMetricsManager.fetchLimiterWaitingTaskNumSupplier = fetchLimiterWaitingTaskNumSupplier;
    }

    public static void setFetchPendingTaskNumSupplier(Supplier<Map<String, Integer>> fetchPendingTaskNumSupplier) {
        S3StreamKafkaMetricsManager.fetchPendingTaskNumSupplier = fetchPendingTaskNumSupplier;
    }

    public static CounterMetric buildFetchLimiterTimeoutMetric(String limiterName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, FETCH_LIMITER_ATTRIBUTES.get(limiterName), () -> fetchLimiterTimeoutCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildFetchLimiterTimeMetric(MetricsLevel metricsLevel, String limiterName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig, FETCH_LIMITER_ATTRIBUTES.get(limiterName));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            FETCH_LIMITER_TIME_METRICS.add(metric);
            return metric;
        }
    }

    public static void setLogAppendPermitNumSupplier(Supplier<Integer> logAppendPermitNumSupplier) {
        S3StreamKafkaMetricsManager.logAppendPermitNumSupplier = logAppendPermitNumSupplier;
    }

    public static void setSlowBrokerSupplier(Supplier<Map<Integer, Boolean>> slowBrokerSupplier) {
        S3StreamKafkaMetricsManager.slowBrokerSupplier = slowBrokerSupplier;
    }

    public static void setPartitionStatusStatisticsSupplier(List<String> partitionStatusList, Function<String, Integer> partitionStatusStatisticsSupplier) {
        S3StreamKafkaMetricsManager.partitionStatusList = partitionStatusList;
        S3StreamKafkaMetricsManager.partitionStatusStatisticsSupplier = partitionStatusStatisticsSupplier;
    }

    public static void setTopicPartitionCountMetricsSupplier(Supplier<PartitionCountDistribution> topicPartitionCountSupplier) {
        S3StreamKafkaMetricsManager.topicPartitionCountSupplier = topicPartitionCountSupplier;
    }

    public static void setBackPressureStateSupplier(Supplier<Map<String, Integer>> backPressureStateSupplier) {
        S3StreamKafkaMetricsManager.backPressureStateSupplier = backPressureStateSupplier;
    }

    public static void setTruststoreCertsSupplier(Supplier<String> truststoreCertsSupplier) {
        S3StreamKafkaMetricsManager.truststoreCertsSupplier = truststoreCertsSupplier;
    }

    public static void setCertChainSupplier(Supplier<String> certChainSupplier) {
        S3StreamKafkaMetricsManager.certChainSupplier = certChainSupplier;
    }
}
