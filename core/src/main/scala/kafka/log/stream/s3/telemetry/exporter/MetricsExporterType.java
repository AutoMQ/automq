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

package kafka.log.stream.s3.telemetry.exporter;

public enum MetricsExporterType {
    OTLP("otlp"),
    PROMETHEUS("prometheus"),
    OPS("ops"),
    KAFKA("kafka");

    private final String type;

    MetricsExporterType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static MetricsExporterType fromString(String type) {
        for (MetricsExporterType exporterType : MetricsExporterType.values()) {
            if (exporterType.getType().equalsIgnoreCase(type)) {
                return exporterType;
            }
        }
        throw new IllegalArgumentException("Invalid metrics exporter type: " + type);
    }
}
