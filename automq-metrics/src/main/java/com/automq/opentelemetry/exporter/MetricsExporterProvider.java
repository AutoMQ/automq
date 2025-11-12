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

package com.automq.opentelemetry.exporter;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Service Provider Interface that allows extending the available metrics exporters
 * without modifying the core AutoMQ OpenTelemetry module.
 */
public interface MetricsExporterProvider {

    /**
     * @param scheme exporter scheme (e.g. "rw")
     * @return true if this provider can create an exporter for the supplied scheme
     */
    boolean supports(String scheme);

    /**
     * Creates a metrics exporter for the provided URI.
     *
     * @param config          metrics configuration
     * @param uri             original exporter URI
     * @param queryParameters parsed query parameters from the URI
     * @return a MetricsExporter instance, or {@code null} if unable to create one
     */
    MetricsExporter create(MetricsExportConfig config, URI uri, Map<String, List<String>> queryParameters);
}
