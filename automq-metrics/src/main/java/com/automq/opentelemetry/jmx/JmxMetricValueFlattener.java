/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.opentelemetry.jmx;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

final class JmxMetricValueFlattener {
    static final int MAX_IDENTITY_LENGTH = 4096;
    static final int MAX_LABEL_LENGTH = 1024;
    private final int maxDepth;
    private final int maxElements;
    private final int maxPoints;
    private final List<Point> points = new ArrayList<>();
    private final Set<Object> ancestors = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private long dropped;
    private long unsupported;

    JmxMetricValueFlattener(int maxDepth, int maxElements, int maxPoints) {
        this.maxDepth = maxDepth;
        this.maxElements = maxElements;
        this.maxPoints = maxPoints;
    }

    void flatten(String path, Object value, Attributes attributes) {
        flatten(path, value, attributes, 0);
    }

    private void flatten(String path, Object value, Attributes attributes, int depth) {
        if (value == null) {
            return;
        }
        if (points.size() >= maxPoints || depth > maxDepth || path.length() > MAX_IDENTITY_LENGTH
            || Thread.currentThread().isInterrupted()) {
            dropped++;
            return;
        }
        if (value instanceof Number || value instanceof Boolean) {
            double number = value instanceof Boolean ? ((Boolean) value ? 1 : 0) : ((Number) value).doubleValue();
            if (Double.isFinite(number)) {
                points.add(new Point(path, number, attributes.toBuilder().put("jmx_attribute", path).build()));
            } else {
                dropped++;
            }
            return;
        }
        if (!ancestors.add(value)) {
            dropped++;
            return;
        }
        try {
            flattenStructure(path, value, attributes, depth);
        } finally {
            ancestors.remove(value);
        }
    }

    private void flattenStructure(String path, Object value, Attributes attributes, int depth) {
        if (value instanceof CompositeData) {
            CompositeData composite = (CompositeData) value;
            int count = 0;
            for (String key : new TreeSet<>(composite.getCompositeType().keySet())) {
                if (count++ >= maxElements) {
                    dropped++;
                    break;
                }
                flatten(path + "/" + JmxMetricNaming.pathPart(key), composite.get(key), attributes, depth + 1);
            }
        } else if (value instanceof TabularData) {
            flattenTable(path, (TabularData) value, attributes, depth);
        } else if (value instanceof Map) {
            int count = 0;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (count++ >= maxElements) {
                    dropped++;
                    break;
                }
                String key = scalarKey(entry.getKey());
                if (key == null) {
                    unsupported++;
                } else {
                    Attributes keyed = attributes.toBuilder().put("jmx_key_" + depth, key)
                        .put("jmx_key_type_" + depth, entry.getKey().getClass().getName()).build();
                    flatten(path, entry.getValue(), keyed, depth + 1);
                }
            }
        } else if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            for (int index = 0; index < Math.min(length, maxElements); index++) {
                Attributes indexed = attributes.toBuilder().put("jmx_index_" + depth, Integer.toString(index)).build();
                flatten(path, Array.get(value, index), indexed, depth + 1);
            }
            if (length > maxElements) {
                dropped++;
            }
        } else {
            unsupported++;
        }
    }

    private void flattenTable(String path, TabularData table, Attributes attributes, int depth) {
        Collection<?> rows = table.values();
        List<String> indices = table.getTabularType().getIndexNames();
        int count = 0;
        for (Object row : rows) {
            if (count++ >= maxElements) {
                dropped++;
                break;
            }
            if (!(row instanceof CompositeData)) {
                unsupported++;
                continue;
            }
            CompositeData composite = (CompositeData) row;
            AttributesBuilder labels = attributes.toBuilder();
            boolean valid = true;
            for (String index : indices) {
                String key = scalarKey(composite.get(index));
                if (key == null || index.length() > MAX_LABEL_LENGTH) {
                    valid = false;
                    unsupported++;
                    break;
                }
                labels.put("jmx_row_" + JmxMetricNaming.labelPart(index) + "_" + depth, key);
            }
            if (!valid) {
                continue;
            }
            int columnCount = 0;
            for (String column : new TreeSet<>(composite.getCompositeType().keySet())) {
                if (indices.contains(column)) {
                    continue;
                }
                if (columnCount++ >= maxElements) {
                    dropped++;
                    break;
                }
                String columnPath = "value".equals(column) ? path : path + "/" + JmxMetricNaming.pathPart(column);
                flatten(columnPath, composite.get(column), labels.build(), depth + 1);
            }
        }
    }

    private String scalarKey(Object key) {
        if (key instanceof String || key instanceof Number || key instanceof Boolean || key instanceof Character) {
            String value = key.toString();
            if (value.length() <= MAX_LABEL_LENGTH) {
                return value;
            }
            dropped++;
        }
        return null;
    }

    List<Point> points() {
        return List.copyOf(points);
    }

    long dropped() {
        return dropped;
    }

    long unsupported() {
        return unsupported;
    }

    static final class Point {
        final String path;
        final double value;
        final Attributes attributes;

        Point(String path, double value, Attributes attributes) {
            this.path = path;
            this.value = value;
            this.attributes = attributes;
        }
    }
}
