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

package kafka.server.metrics;

import java.util.Date;
import java.util.function.Supplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

public final class LicenseMetricsManager {
    private static volatile Supplier<Date> expireDateSupplier;

    private LicenseMetricsManager() {
    }

    public static void setExpireDateSupplier(Supplier<Date> supplier) {
        expireDateSupplier = supplier;
    }

    public static void initMetrics(Meter meter) {
        meter.gaugeBuilder("license_expiry_timestamp")
            .setDescription("The expiry timestamp of the license")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> {
                if (expireDateSupplier != null) {
                    Date expireDate = expireDateSupplier.get();
                    if (expireDate != null) {
                        measurement.record(expireDate.getTime(), Attributes.empty());
                    }
                }
            });

        meter.gaugeBuilder("license_seconds_remaining")
            .setDescription("The remaining seconds until the license expires")
            .setUnit("seconds")
            .ofLongs()
            .buildWithCallback(measurement -> {
                if (expireDateSupplier != null) {
                    Date expireDate = expireDateSupplier.get();
                    if (expireDate != null) {
                        long secondsRemaining = (expireDate.getTime() - System.currentTimeMillis()) / 1000;
                        measurement.record(secondsRemaining, Attributes.empty());
                    }
                }
            });

        meter.gaugeBuilder("node_vcpu_count")
            .setDescription("The number of vCPUs available on this node")
            .setUnit("count")
            .ofLongs()
            .buildWithCallback(measurement -> {
                measurement.record(Runtime.getRuntime().availableProcessors(), Attributes.empty());
            });
    }
}
