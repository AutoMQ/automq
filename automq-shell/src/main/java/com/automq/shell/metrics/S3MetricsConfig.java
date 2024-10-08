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

package com.automq.shell.metrics;

import com.automq.stream.s3.operator.ObjectStorage;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface S3MetricsConfig {

    String clusterId();

    boolean isActiveController();

    int nodeId();

    ObjectStorage objectStorage();

    List<Pair<String, String>> baseLabels();
}
