/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
