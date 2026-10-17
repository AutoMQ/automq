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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("S3Unit")
public class KafkaCertificateMetricsTest {

    /** Verifies empty PEM content produces no certificates. */
    @Test
    public void testParseCertificatesWithEmptyString() throws Exception {
        X509Certificate[] certificates = KafkaCertificateMetrics.parseCertificates("");

        assertNotNull(certificates);
        assertEquals(0, certificates.length);
    }

    /** Verifies PEM Base64 whitespace is ignored before decoding. */
    @Test
    public void testParseCertificatesWithWhitespaceInBase64() {
        String certWithSpaces = "-----BEGIN CERTIFICATE-----\n"
            + "TUlJQmtUQ0IrUFNKQnFaUUhpUWxDd0ZBTUJReEVqQVFCZ05W"
            + " QkFNTUNXeHZZMkZzYUc5emREQWVGdzB5TlRFd01qbHhNREF3TUZG\n"
            + "QUFNVUNXeHZZMkZzYUc5emREQWVGdzB5TlRFd01qbHhNREF3\t"
            + "TUZGUUFNVUNXeHZZMG\r\n"
            + "-----END CERTIFICATE-----";

        assertDoesNotThrow(() -> {
            X509Certificate[] certificates = KafkaCertificateMetrics.parseCertificates(certWithSpaces);
            assertNotNull(certificates);
        });
    }

    /** Verifies invalid Base64 certificate content is skipped without failing setup. */
    @Test
    public void testParseCertificatesWithInvalidBase64() {
        String invalidCert = "-----BEGIN CERTIFICATE-----\n"
            + "InvalidBase64Content!!!\n"
            + "-----END CERTIFICATE-----";

        assertDoesNotThrow(() -> {
            X509Certificate[] certificates = KafkaCertificateMetrics.parseCertificates(invalidCert);
            assertNotNull(certificates);
            assertEquals(0, certificates.length);
        });
    }
}
