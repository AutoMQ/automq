/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry.exporter;

public enum OTLPCompressionType {
    GZIP("gzip"),
    NONE("none");

    private final String type;

    OTLPCompressionType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static OTLPCompressionType fromString(String type) {
        for (OTLPCompressionType compressionType : OTLPCompressionType.values()) {
            if (compressionType.getType().equalsIgnoreCase(type)) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("Invalid OTLP compression type: " + type);
    }
}
