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

package org.apache.kafka.server.metrics.s3stream;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class S3StreamKafkaMetricsManagerTest {

    @Test
    public void testParseCertificatesWithEmptyString() throws Exception {
        X509Certificate[] certificates = callParseCertificates("");
        
        assertNotNull(certificates);
        assertEquals(0, certificates.length);
    }

    @Test
    public void testParseCertificatesWithWhitespaceInBase64() throws Exception {
        // Test certificate with whitespace in Base64 content that would cause "Illegal base64 character 20" error
        String certWithSpaces = "-----BEGIN CERTIFICATE-----\n" +
                               "TUlJQmtUQ0IrUFNKQnFaUUhpUWxDd0ZBTUJReEVqQVFCZ05W" + // base64 line with spaces
                               " QkFNTUNXeHZZMkZzYUc5emREQWVGdzB5TlRFd01qbHhNREF3TUZG\n" + // Leading space
                               "QUFNVUNXeHZZMkZzYUc5emREQWVGdzB5TlRFd01qbHhNREF3\t" + // Trailing tab
                               "TUZGUUFNVUNXeHZZMG\r\n" + // Carriage return + newline
                               "-----END CERTIFICATE-----";
        
        // This should not throw IllegalArgumentException due to the fix
        assertDoesNotThrow(() -> {
            X509Certificate[] certificates = callParseCertificates(certWithSpaces);
            assertNotNull(certificates);
            // The certificate might not be valid (just test data), but at least it shouldn't crash with Base64 error
        });
    }

    @Test
    public void testParseCertificatesWithInvalidBase64() throws Exception {
        String invalidCert = "-----BEGIN CERTIFICATE-----\n" +
                             "InvalidBase64Content!!!\n" +
                             "-----END CERTIFICATE-----";
        
        // Should not throw exception but return empty array due to graceful error handling
        assertDoesNotThrow(() -> {
            X509Certificate[] certificates = callParseCertificates(invalidCert);
            assertNotNull(certificates);
            assertEquals(0, certificates.length); // Invalid cert should be skipped
        });
    }

    /**
     * Helper method to call the private parseCertificates method using reflection
     */
    private X509Certificate[] callParseCertificates(String pemContent) throws Exception {
        Method method = S3StreamKafkaMetricsManager.class.getDeclaredMethod("parseCertificates", String.class);
        method.setAccessible(true);
        return (X509Certificate[]) method.invoke(null, pemContent);
    }
}
