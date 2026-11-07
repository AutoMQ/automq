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

package com.automq.stream.s3.metrics.operations;

import java.util.Map;

public enum S3MetricsType {
    S3Stream("S3Stream"),
    S3Storage("S3Storage"),
    S3Request("S3Request"),
    S3Object("S3Object"),
    S3Network("S3Network");

    private static final Map<String, S3MetricsType> MAP = Map.of(
        "S3Stream", S3Stream,
        "S3Storage", S3Storage,
        "S3Request", S3Request,
        "S3Object", S3Object
    );

    private final String name;

    S3MetricsType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public S3MetricsType of(String name) {
        return MAP.get(name);
    }

    @Override
    public String toString() {
        return "S3MetricsType{" +
            "name='" + name + '\'' +
            '}';
    }
}
