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

package org.apache.kafka.server.record;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public enum TableTopicType {
    SCHEMALESS("schemaless"),
    REGISTRY_SCHEMA("registry_schema");

    public final String name;
    private static final List<TableTopicType> VALUES = asList(values());

    TableTopicType(String name) {
        this.name = name;
    }

    public static List<String> names() {
        return VALUES.stream().map(v -> v.name).collect(Collectors.toList());
    }

    public static TableTopicType forName(String n) {
        String name = n.toLowerCase(Locale.ROOT);
        return VALUES.stream().filter(v -> v.name.equals(name)).findFirst().orElseThrow(() ->
            new IllegalArgumentException("Unknown table topic type name: " + name)
        );
    }
}
