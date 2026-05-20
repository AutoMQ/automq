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

package com.automq.stream.s3.metrics.stats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokerResourceStatsTest {
    private final BrokerResourceStats stats = BrokerResourceStats.getInstance();

    @AfterEach
    public void tearDown() {
        stats.reset();
    }

    @Test
    public void testDefaultUnavailable() {
        Assertions.assertTrue(Double.isNaN(stats.cpuUtil()));
        Assertions.assertTrue(Double.isNaN(stats.networkUtil()));
    }

    @Test
    public void testSupplierReplacement() {
        stats.setCpuUtilSupplier(() -> 0.3);
        stats.setNetworkUtilSupplier(() -> 0.7);
        Assertions.assertEquals(0.3, stats.cpuUtil());
        Assertions.assertEquals(0.7, stats.networkUtil());

        stats.setCpuUtilSupplier(() -> 0.4);
        stats.setNetworkUtilSupplier(() -> 0.8);
        Assertions.assertEquals(0.4, stats.cpuUtil());
        Assertions.assertEquals(0.8, stats.networkUtil());
    }

    @Test
    public void testSupplierExceptionReturnsNaN() {
        stats.setCpuUtilSupplier(() -> {
            throw new RuntimeException("cpu unavailable");
        });
        stats.setNetworkUtilSupplier(() -> {
            throw new RuntimeException("network unavailable");
        });

        Assertions.assertTrue(Double.isNaN(stats.cpuUtil()));
        Assertions.assertTrue(Double.isNaN(stats.networkUtil()));
    }

    @Test
    public void testRawInvalidValuePassThrough() {
        stats.setCpuUtilSupplier(() -> -1.0);
        stats.setNetworkUtilSupplier(() -> Double.POSITIVE_INFINITY);

        Assertions.assertEquals(-1.0, stats.cpuUtil());
        Assertions.assertEquals(Double.POSITIVE_INFINITY, stats.networkUtil());
    }
}
