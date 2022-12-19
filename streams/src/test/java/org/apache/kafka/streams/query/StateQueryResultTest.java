/*
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
package org.apache.kafka.streams.query;

import org.apache.kafka.streams.query.internals.SucceededQueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;


class StateQueryResultTest {

    StateQueryResult<String> stringStateQueryResult;
    final QueryResult<String> noResultsFound = new SucceededQueryResult<>(null);
    final QueryResult<String> validResult = new SucceededQueryResult<>("Foo");

    @BeforeEach
    public void setUp() {
        stringStateQueryResult = new StateQueryResult<>();
    }

    @Test
    @DisplayName("Zero query results shouldn't error")
    void getOnlyPartitionResultNoResultsTest() {
        stringStateQueryResult.addResult(0, noResultsFound);
        final QueryResult<String> result = stringStateQueryResult.getOnlyPartitionResult();
        assertThat(result, nullValue());
    }

    @Test
    @DisplayName("Valid query results still works")
    void getOnlyPartitionResultWithSingleResultTest() {
        stringStateQueryResult.addResult(0, validResult);
        final QueryResult<String> result = stringStateQueryResult.getOnlyPartitionResult();
        assertThat(result.getResult(), is("Foo"));
    }

    @Test
    @DisplayName("More than one query result throws IllegalArgumentException ")
    void getOnlyPartitionResultMultipleResults() {
        stringStateQueryResult.addResult(0, validResult);
        stringStateQueryResult.addResult(1, validResult);
        assertThrows(IllegalArgumentException.class, () -> stringStateQueryResult.getOnlyPartitionResult());
    }
}