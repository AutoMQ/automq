/*
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

package org.apache.kafka.connect.automq.s3;

import org.apache.kafka.connect.automq.log.LogConfigConstants;
import org.apache.kafka.connect.automq.metrics.MetricsConfigConstants;

import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3PermissionProbeTest {

    @BeforeAll
    public static void registerTestProtocol() {
        ObjectStorageFactory.instance().registerProtocolHandler("testfail", builder -> new FailingMemoryObjectStorage(builder.bucket().bucketId()));
    }

    @Test
    public void logProbeNotRequiredWhenDisabled() {
        Map<String, String> props = Map.of(LogConfigConstants.LOG_S3_ENABLE_KEY, "false");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeLogUploader(props);
        assertFalse(result.isRequired());
        assertFalse(result.shouldInitialize());
    }

    @Test
    public void logProbeFailsWithoutBucket() {
        Map<String, String> props = Map.of(LogConfigConstants.LOG_S3_ENABLE_KEY, "true");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeLogUploader(props);
        assertTrue(result.isRequired());
        assertFalse(result.shouldInitialize());
        assertNotNull(result.reason());
    }

    @Test
    public void logProbeReadsMemoryBucket() {
        Map<String, String> props = new HashMap<>();
        props.put(LogConfigConstants.LOG_S3_ENABLE_KEY, "true");
        props.put(LogConfigConstants.LOG_S3_BUCKET_KEY, "0@mem://log-bucket");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeLogUploader(props);
        assertTrue(result.shouldInitialize());
    }

    @Test
    public void metricsProbeSkipsNonOpsExporter() {
        Map<String, String> props = Map.of(MetricsConfigConstants.EXPORTER_URI_KEY, "prometheus://0.0.0.0:9090");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeMetrics(props);
        assertFalse(result.isRequired());
    }

    @Test
    public void metricsProbeFailsWithoutBucket() {
        Map<String, String> props = new HashMap<>();
        props.put(MetricsConfigConstants.EXPORTER_URI_KEY, "ops://bucket");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeMetrics(props);
        assertTrue(result.isRequired());
        assertFalse(result.shouldInitialize());
        assertNotNull(result.reason());
    }

    @Test
    public void metricsProbeReadsMemoryBucket() {
        Map<String, String> props = new HashMap<>();
        props.put(MetricsConfigConstants.EXPORTER_URI_KEY, "ops://bucket");
        props.put(MetricsConfigConstants.S3_BUCKET, "0@mem://metrics-bucket");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeMetrics(props);
        assertTrue(result.shouldInitialize());
    }

    @Test
    public void metricsProbeDetectsOpsExporterWhenNotFirst() {
        Map<String, String> props = new HashMap<>();
        props.put(MetricsConfigConstants.EXPORTER_URI_KEY, "prometheus://0.0.0.0:9090,ops://bucket");
        props.put(MetricsConfigConstants.S3_BUCKET, "0@mem://metrics-bucket");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeMetrics(props);
        assertTrue(result.isRequired());
        assertTrue(result.shouldInitialize());
    }

    @Test
    public void metricsProbeFailsWhenOpsBucketMissingEvenIfOtherExportersConfigured() {
        Map<String, String> props = new HashMap<>();
        props.put(MetricsConfigConstants.EXPORTER_URI_KEY, "prometheus://localhost:9090, ops://bucket");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeMetrics(props);
        assertTrue(result.isRequired());
        assertFalse(result.shouldInitialize());
        assertNotNull(result.reason());
    }

    @Test
    public void readinessFailurePreventsInitialization() {
        Map<String, String> props = new HashMap<>();
        props.put(LogConfigConstants.LOG_S3_ENABLE_KEY, "true");
        props.put(LogConfigConstants.LOG_S3_BUCKET_KEY, "0@testfail://log-bucket");
        S3PermissionProbe.ProbeResult result = S3PermissionProbe.probeLogUploader(props);
        assertTrue(result.isRequired());
        assertFalse(result.shouldInitialize());
        assertTrue(result.reason().contains("not ready"));
    }

    private static class FailingMemoryObjectStorage extends MemoryObjectStorage {
        FailingMemoryObjectStorage(short bucketId) {
            super(bucketId);
        }

        @Override
        public boolean readinessCheck() {
            return false;
        }
    }
}
