/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.server;

import org.apache.kafka.common.config.types.Password;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.function.LongSupplier;

import io.opentelemetry.api.common.Attributes;

/**
 * Registers Kafka TLS certificate metrics from broker configuration.
 */
public final class KafkaCertificateMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCertificateMetrics.class);
    private static final String SERVER_CERT_TYPE = "server_cert";
    private static final String TRUSTSTORE_CERT_TYPE = "truststore_cert";
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;
    private static final Metrics.LongGaugeBundle CERT_EXPIRY_TIMESTAMP = Metrics.instance()
        .longGauge("kafka_stream_cert_expiry_timestamp", "The expiry timestamp of the TLS certificate", "milliseconds");
    private static final Metrics.LongGaugeBundle CERT_DAYS_REMAINING = Metrics.instance()
        .longGauge("kafka_stream_cert_days_remaining", "The remaining days until the TLS certificate expires", "");

    private KafkaCertificateMetrics() {
    }

    /**
     * Reads TLS certificate configuration and registers certificate gauges when both server and truststore
     * certificates are configured.
     */
    public static void setup(KafkaConfig config) {
        String truststoreCerts = certificateValue(config, "ssl.truststore.certificates", "truststore certificates");
        String certChain = certificateValue(config, "ssl.keystore.certificate.chain", "certificate chain");
        if (truststoreCerts == null || truststoreCerts.isEmpty()) {
            return;
        }
        if (certChain == null || certChain.isEmpty()) {
            return;
        }

        try {
            for (X509Certificate cert : parseCertificates(certChain)) {
                registerCertMetrics(cert, SERVER_CERT_TYPE);
            }
            for (X509Certificate cert : parseCertificates(truststoreCerts)) {
                registerCertMetrics(cert, TRUSTSTORE_CERT_TYPE);
            }
        } catch (CertificateException e) {
            LOGGER.error("Failed to init cert metrics", e);
        }
    }

    static X509Certificate[] parseCertificates(String pemContent) throws CertificateException {
        String[] pemArray = pemContent.split("-----END CERTIFICATE-----");
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        List<X509Certificate> certList = new ArrayList<>();

        for (String pemPart : pemArray) {
            String cleanedPemPart = pemPart.replace("-----BEGIN CERTIFICATE-----", "")
                .replaceAll("\\s", "");
            if (cleanedPemPart.isEmpty()) {
                continue;
            }

            try {
                byte[] certBytes = Base64.getDecoder().decode(cleanedPemPart);
                X509Certificate cert = (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
                certList.add(cert);
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Failed to decode certificate part due to invalid Base64, skipping: {}", e.getMessage());
            } catch (CertificateException e) {
                LOGGER.warn("Failed to parse certificate, skipping: {}", e.getMessage());
            }
        }

        return certList.toArray(new X509Certificate[0]);
    }

    private static String certificateValue(KafkaConfig config, String configName, String description) {
        try {
            Password password = config.getPassword(configName);
            return password != null ? password.value() : null;
        } catch (Exception e) {
            LOGGER.error("Failed to obtain {}", description, e);
            return null;
        }
    }

    private static void registerCertMetrics(X509Certificate cert, String certType) {
        String subject = cert.getSubjectX500Principal().getName();
        Date expiryDate = cert.getNotAfter();
        Attributes attributes = Attributes.builder()
            .put("cert_type", certType)
            .put("cert_subject", subject)
            .build();

        CERT_EXPIRY_TIMESTAMP.register(MetricsLevel.INFO, attributes).record(expiryDate.getTime());
        CERT_DAYS_REMAINING.register(MetricsLevel.INFO, attributes).record(daysRemainingSupplier(expiryDate.getTime()));
    }

    private static LongSupplier daysRemainingSupplier(long expiryTimeMs) {
        return () -> (expiryTimeMs - System.currentTimeMillis()) / MILLIS_PER_DAY;
    }
}
