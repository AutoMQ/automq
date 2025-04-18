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

package org.apache.kafka.server.metrics.cert;

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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

public class CertKafkaMetricsManager {
    // Logger for logging messages related to this class
    private static final Logger LOGGER = LoggerFactory.getLogger(CertKafkaMetricsManager.class);

    // List to store all the observable long gauges for certificates
    private static final List<ObservableLongGauge> CERT_OBSERVABLE_LONG_GAUGES = new ArrayList<>();

    /**
     * Initialize the certificate metrics.
     *
     * @param meter The OpenTelemetry meter to use for creating metrics.
     * @param truststoreCerts The truststore certificates in PEM format.
     * @param certChain The certificate chain in PEM format.
     */
    public static void initMetrics(Meter meter, String truststoreCerts, String certChain, String prefix) {
        try {
            if (truststoreCerts == null || truststoreCerts.isEmpty()) {
                LOGGER.warn("Truststore certificates are empty or null");
                return;
            }
            if (certChain == null || certChain.isEmpty()) {
                LOGGER.warn("Certificate chain is empty or null");
                return;
            }
            // Add TLS certificate metrics
            addTlsMetrics(certChain, truststoreCerts, meter, prefix);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize cert metrics", e);
        }
    }

    /**
     * Add TLS certificate metrics.
     *
     * @param certChain The certificate chain in PEM format.
     * @param truststoreCerts The truststore certificates in PEM format.
     * @param meter The OpenTelemetry meter to use for creating metrics.
     * @param prefix The prefix for the metric names.
     */
    private static void addTlsMetrics(String certChain, String truststoreCerts, Meter meter, String prefix) {
        try {

            // Parse and check the certificate expiration time
            X509Certificate[] serverCerts = parseCertificates(certChain);
            X509Certificate[] trustStoreCerts = parseCertificates(truststoreCerts);

            for (X509Certificate cert : serverCerts) {
                registerCertMetrics(meter, cert, "server_cert", prefix);
            }
            for (X509Certificate cert : trustStoreCerts) {
                registerCertMetrics(meter, cert, "truststore_cert", prefix);
            }

        } catch (Exception e) {
            LOGGER.error("Failed to add TLS metrics", e);
        }
    }

    /**
     * Register certificate metrics.
     *
     * @param meter The OpenTelemetry meter to use for creating metrics.
     * @param cert The X509 certificate to register metrics for.
     * @param certType The type of the certificate (e.g., "server_cert", "truststore_cert").
     * @param prefix The prefix for the metric names.
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

        ObservableLongGauge observableCertExpireMills = meter.gaugeBuilder(prefix + "expiry_timestamp")
                .setDescription("The expiry timestamp of the TLS certificate")
                .setUnit("milliseconds")
                .ofLongs()
                .buildWithCallback(result -> result.record(expiryDate.getTime(), attributes));
        CERT_OBSERVABLE_LONG_GAUGES.add(observableCertExpireMills);

        ObservableLongGauge observableCertExpireDays = meter.gaugeBuilder(prefix + "days_remaining")
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
}