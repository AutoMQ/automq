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

package com.automq.opentelemetry.exporter.s3;

import com.automq.stream.s3.operator.ObjectStorage;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Configuration interface for S3 metrics exporter.
 */
public interface S3MetricsConfig {

    /**
     * Get the cluster ID.
     * @return The cluster ID.
     */
    String clusterId();

    /**
     * Check if the current node is a primary node for metrics upload.
     * @return True if the current node should upload metrics, false otherwise.
     */
    boolean isPrimaryUploader();

    /**
     * Get the node ID.
     * @return The node ID.
     */
    int nodeId();

    /**
     * Get the object storage instance.
     * @return The object storage instance.
     */
    ObjectStorage objectStorage();

    /**
     * Get the base labels to include in all metrics.
     * @return The base labels.
     */
    List<Pair<String, String>> baseLabels();
}
