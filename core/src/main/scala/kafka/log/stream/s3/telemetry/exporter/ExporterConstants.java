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
