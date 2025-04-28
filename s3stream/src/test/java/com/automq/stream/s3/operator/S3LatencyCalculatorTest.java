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

package com.automq.stream.s3.operator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3LatencyCalculatorTest {

    @Test
    public void test() {
        long[] buckets = {10, 20};
        long highestValue = 100;
        S3LatencyCalculator calculator = new S3LatencyCalculator(buckets, highestValue);

        assertEquals(0, calculator.valueAtPercentile(4, 75));
        assertEquals(0, calculator.valueAtPercentile(12, 75));
        assertEquals(0, calculator.valueAtPercentile(24, 75));

        calculator.record(4, 10);
        assertEquals(5, calculator.valueAtPercentile(5, 75));
        assertEquals(0, calculator.valueAtPercentile(15, 75));
        assertEquals(0, calculator.valueAtPercentile(25, 75));

        calculator.record(12, 10);
        assertEquals(10, calculator.valueAtPercentile(5, 75));
        assertEquals(5, calculator.valueAtPercentile(15, 75));
        assertEquals(0, calculator.valueAtPercentile(25, 75));

        calculator.record(24, 10);
        assertEquals(10, calculator.valueAtPercentile(5, 75));
        assertEquals(10, calculator.valueAtPercentile(15, 75));
        assertEquals(10, calculator.valueAtPercentile(25, 75));

        calculator.record(300, 10);
        assertEquals(10, calculator.valueAtPercentile(5, 75));
        assertEquals(10, calculator.valueAtPercentile(15, 75));
        assertEquals(10, calculator.valueAtPercentile(25, 75));
    }
}
