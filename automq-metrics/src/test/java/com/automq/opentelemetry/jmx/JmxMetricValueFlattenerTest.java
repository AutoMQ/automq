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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class JmxMetricValueFlattenerTest {
    /** Standard Open MBean structures, ordinary maps and numeric arrays expose numeric leaves only. */
    @Test
    void testOpenTypesMapsAndArrays() throws Exception {
        JmxMetricValueFlattener flattener = new JmxMetricValueFlattener(8, 32, 100);
        CompositeType memory = new CompositeType("Memory", "Memory", new String[] {"used", "text"},
            new String[] {"Used", "Text"}, new OpenType<?>[] {SimpleType.LONG, SimpleType.STRING});
        flattener.flatten("Memory", new CompositeDataSupport(memory, Map.of("used", 23L, "text", "not a label")), Attributes.empty());
        CompositeType row = new CompositeType("Row", "Row", new String[] {"key", "value"},
            new String[] {"Key", "Value"}, new OpenType<?>[] {SimpleType.STRING, SimpleType.LONG});
        TabularDataSupport table = new TabularDataSupport(new TabularType("Table", "Table", row, new String[] {"key"}));
        table.put(new CompositeDataSupport(row, Map.of("key", "db.table", "value", 41L)));
        flattener.flatten("RowsScanned", table, Attributes.empty());
        flattener.flatten("Map", Map.of("table", 3L, "string", "not a label"), Attributes.empty());
        flattener.flatten("Array", new double[] {1, 2}, Attributes.empty());
        flattener.flatten("Flag", true, Attributes.empty());
        List<JmxMetricValueFlattener.Point> points = flattener.points();
        assertEquals(6, points.size());
        assertEquals(23, points.get(0).value);
        assertEquals("Memory/used", points.get(0).path);
        assertEquals("db.table", points.get(1).attributes.get(AttributeKey.stringKey("jmx_row_key_0")));
        assertEquals("table", points.get(2).attributes.get(AttributeKey.stringKey("jmx_key_0")));
        assertEquals("0", points.get(3).attributes.get(AttributeKey.stringKey("jmx_index_0")));
        assertEquals(2, flattener.unsupported());
        assertTrue(points.stream().noneMatch(point -> point.attributes.toString().contains("not a label")));
    }

    /** Recursive graphs, oversized identities and nonfinite numbers stay within fixed limits. */
    @Test
    void testCyclesBudgetsAndInvalidLeaves() {
        JmxMetricValueFlattener flattener = new JmxMetricValueFlattener(2, 3, 2);
        Map<String, Object> recursive = new HashMap<>();
        recursive.put("self", recursive);
        flattener.flatten("Recursive", recursive, Attributes.empty());
        flattener.flatten("Huge", Map.of("x".repeat(1100), 1), Attributes.empty());
        flattener.flatten("Nan", Double.NaN, Attributes.empty());
        flattener.flatten("Numbers", new long[] {1, 2, 3, 4, 5}, Attributes.empty());
        assertEquals(2, flattener.points().size());
        assertTrue(flattener.dropped() >= 4);
    }

    /** Normalization, case folding and path separators cannot merge distinct metric identities. */
    @Test
    void testStableCollisionResistantNamesAndLabels() {
        String name = JmxMetricNaming.metricName("vendor.domain", "Value");
        assertEquals(name, JmxMetricNaming.metricName("vendor.domain", "Value"));
        assertTrue(name.startsWith("connect_jmx_vendor_domain_value_"));
        assertTrue(name.endsWith("_value"));
        assertNotEquals(name, JmxMetricNaming.metricName("vendor_domain", "Value"));
        assertNotEquals(name, JmxMetricNaming.metricName("vendor.domain", "value"));
        assertNotEquals(JmxMetricNaming.labelPart("a-b"), JmxMetricNaming.labelPart("a_2d_b"));
        assertEquals("a~1b~0c", JmxMetricNaming.pathPart("a/b~c"));
    }

    /** Equal string representations of different native map-key types remain separate series. */
    @Test
    void testMixedScalarMapKeysKeepTheirTypes() {
        JmxMetricValueFlattener flattener = new JmxMetricValueFlattener(4, 8, 8);
        Map<Object, Object> values = new HashMap<>();
        values.put(1, 11);
        values.put(1L, 12);
        values.put("1", 13);
        flattener.flatten("Mixed", values, Attributes.empty());
        assertEquals(3, flattener.points().size());
        assertEquals(3, flattener.points().stream().map(point -> point.attributes).distinct().count());
        assertTrue(flattener.points().stream().allMatch(point -> "1".equals(point.attributes.get(AttributeKey.stringKey("jmx_key_0")))));
    }
}
