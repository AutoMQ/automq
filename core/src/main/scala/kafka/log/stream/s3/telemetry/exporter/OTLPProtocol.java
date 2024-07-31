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

public enum OTLPProtocol {
    GRPC("grpc"),
    HTTP("http");

    private final String protocol;

    OTLPProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getProtocol() {
        return protocol;
    }

    public static OTLPProtocol fromString(String protocol) {
        for (OTLPProtocol otlpProtocol : OTLPProtocol.values()) {
            if (otlpProtocol.getProtocol().equalsIgnoreCase(protocol)) {
                return otlpProtocol;
            }
        }
        throw new IllegalArgumentException("Invalid OTLP protocol: " + protocol);
    }
}
