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

package org.apache.kafka.server.common.automq;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.common.automq.TableTopicConfigValidator.IdColumnsValidator;
import org.apache.kafka.server.common.automq.TableTopicConfigValidator.PartitionValidator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.kafka.server.common.automq.TableTopicConfigValidator.COMMA_NO_PARENS_REGEX;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class TableTopicConfigValidatorTest {

    @Test
    public void testPartitionValidator() {
        // not an array
        assertThrowsExactly(ConfigException.class, () -> PartitionValidator.INSTANCE.ensureValid("config_name", "year(column_name)"));
        // invalid transform function
        assertThrowsExactly(ConfigException.class, () -> PartitionValidator.INSTANCE.ensureValid("config_name", "[clock(column_name)]"));
        // invalid column name
        assertThrowsExactly(ConfigException.class, () -> PartitionValidator.INSTANCE.ensureValid("config_name", "[year(\"column_name\")]"));
        // invalid bucket
        assertThrowsExactly(ConfigException.class, () -> PartitionValidator.INSTANCE.ensureValid("config_name", "[bucket(column_name, abc)]"));
        // invalid truncate
        assertThrowsExactly(ConfigException.class, () -> PartitionValidator.INSTANCE.ensureValid("config_name", "[truncate(column_name, abc)]"));

        // valid
        PartitionValidator.INSTANCE.ensureValid("config_name", "[year(l1.l2.c1), month(c2), day(c3), hour(c4), bucket(c5, 1), truncate(c6, 10)]");
    }

    @Test
    public void testIdColumnsValidator() {
        // not an array
        assertThrowsExactly(ConfigException.class, () -> IdColumnsValidator.INSTANCE.ensureValid("config_name", "c1, c2"));
        // invalid column name
        assertThrowsExactly(ConfigException.class, () -> IdColumnsValidator.INSTANCE.ensureValid("config_name", "[\"c1\", \"c2\"]"));

        // valid
        IdColumnsValidator.INSTANCE.ensureValid("config_name", "[l1.c1, c2]");
    }

    @Test
    public void testStringToList() {
        Assertions.assertEquals(List.of("a", "b", "c"), TableTopicConfigValidator.stringToList("[a, b, c]", COMMA_NO_PARENS_REGEX));
    }
}
