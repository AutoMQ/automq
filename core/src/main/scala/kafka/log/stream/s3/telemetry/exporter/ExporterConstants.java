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

public class ExporterConstants {
    public static final String OTLP_TYPE = "otlp";
    public static final String PROMETHEUS_TYPE = "prometheus";
    public static final String OPS_TYPE = "ops";
    public static final String URI_DELIMITER = "://?";
    public static final String ENDPOINT = "endpoint";
    public static final String PROTOCOL = "protocol";
    public static final String COMPRESSION = "compression";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String COMPRESSION_GZIP = "gzip";
    public static final String COMPRESSION_NONE = "none";
    public static final String OTLP_GRPC_PROTOCOL = "grpc";
    public static final String OTLP_HTTP_PROTOCOL = "http";
    public static final String DEFAULT_PROM_HOST = "localhost";
    public static final int DEFAULT_PROM_PORT = 9090;
    public static final int DEFAULT_EXPORTER_TIMEOUT_MS = 30000;
}
