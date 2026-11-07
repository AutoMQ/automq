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

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Centralizes S3 readiness probing for AutoMQ components that depend on object storage access.
 */
public final class S3PermissionProbe {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3PermissionProbe.class);

    private static final ProbeResult NOT_REQUIRED = new ProbeResult(false, true, null);

    private S3PermissionProbe() {
    }

    public static ProbeResult probeLogUploader(Map<String, String> workerProps) {
        boolean enabled = Boolean.parseBoolean(workerProps.getOrDefault(LogConfigConstants.LOG_S3_ENABLE_KEY, "false"));
        if (!enabled) {
            return NOT_REQUIRED;
        }
        String bucket = workerProps.get(LogConfigConstants.LOG_S3_BUCKET_KEY);
        if (StringUtils.isBlank(bucket)) {
            return ProbeResult.failed("log.s3.bucket is not configured");
        }
        return probeBucket(bucket.trim(), "connect-log-probe");
    }

    public static ProbeResult probeMetrics(Map<String, String> workerProps) {
        String exporterUri = workerProps.get(MetricsConfigConstants.EXPORTER_URI_KEY);
        if (!containsOpsExporter(exporterUri)) {
            // No S3 access needed for non-OPS exporters
            return NOT_REQUIRED;
        }

        String metricBuckets = workerProps.get(MetricsConfigConstants.S3_BUCKET);
        if (StringUtils.isBlank(metricBuckets)) {
            return ProbeResult.failed("automq.telemetry.s3.bucket is not configured");
        }
        try {
            List<BucketURI> bucketURIs = BucketURI.parseBuckets(metricBuckets);
            if (bucketURIs.isEmpty()) {
                return ProbeResult.failed("automq.telemetry.s3.bucket does not contain any valid bucket definitions");
            }
            return probeBucket(bucketURIs.get(0), "connect-metrics-probe");
        } catch (Exception e) {
            return ProbeResult.failed(String.format("Failed to parse automq.telemetry.s3.bucket (%s)", e.getMessage()), e);
        }
    }

    private static ProbeResult probeBucket(String bucketStr, String threadPrefix) {
        try {
            BucketURI bucketURI = BucketURI.parse(bucketStr);
            return probeBucket(bucketURI, threadPrefix);
        } catch (Exception e) {
            return ProbeResult.failed(String.format("Invalid bucket configuration: %s (%s)", bucketStr, e.getMessage()), e);
        }
    }

    private static ProbeResult probeBucket(BucketURI bucketURI, String threadPrefix) {
        ObjectStorage storage = null;
        try {
            storage = ObjectStorageFactory.instance().builder(bucketURI).threadPrefix(threadPrefix).build();
        } catch (Exception e) {
            return ProbeResult.failed(String.format("Failed to create object storage client (%s)", e.getMessage()), e);
        }

        try {
            boolean ready = storage.readinessCheck();
            if (!ready) {
                return ProbeResult.failed("S3 readiness check reported not ready for bucket " + bucketURI.bucket());
            }
            return ProbeResult.ready();
        } catch (Exception e) {
            return ProbeResult.failed(String.format("S3 readiness check failed (%s)", e.getMessage()), e);
        } finally {
            if (storage != null) {
                try {
                    storage.close();
                } catch (Exception closeException) {
                    LOGGER.debug("Failed to close temporary object storage after readiness probe", closeException);
                }
            }
        }
    }

    private static boolean containsOpsExporter(String exporterUri) {
        if (StringUtils.isBlank(exporterUri)) {
            return false;
        }
        String[] exporters = exporterUri.split(",");
        for (String exporter : exporters) {
            String trimmed = exporter.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                URI uri = URI.create(trimmed);
                if (Objects.equals("ops", Optional.ofNullable(uri.getScheme()).map(String::toLowerCase).orElse(null))) {
                    return true;
                }
            } catch (Exception e) {
                LOGGER.warn("Invalid automq.telemetry.exporter.uri entry '{}', assuming non-OPS exporter", trimmed, e);
            }
        }
        return false;
    }

    public static final class ProbeResult {
        private final boolean required;
        private final boolean ready;
        private final String reason;

        private ProbeResult(boolean required, boolean ready, String reason) {
            this.required = required;
            this.ready = ready;
            this.reason = reason;
        }

        public static ProbeResult ready() {
            return new ProbeResult(true, true, null);
        }

        public static ProbeResult failed(String reason) {
            return new ProbeResult(true, false, reason);
        }

        public static ProbeResult failed(String reason, Throwable error) {
            if (error != null) {
                LOGGER.warn(reason, error);
            }
            return failed(reason);
        }

        public boolean isRequired() {
            return required;
        }

        public boolean shouldInitialize() {
            return required && ready;
        }

        public String reason() {
            return reason;
        }
    }
}
