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

package org.apache.kafka.tools.automq.cost;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CostCalculator}.
 * <p>
 * Tests cover zero-data edge cases, known-value computations, savings percentage
 * accuracy, and the effect of different replication and over-provision factors.
 */
@Tag("S3Unit")
public class CostCalculatorTest {

    private static final double DELTA = 0.0001;

    /** Default AWS us-east-1 pricing config for test convenience. */
    private static CostEstimatorConfig defaultConfig() {
        return CostEstimatorConfig.builder("localhost:9092").build();
    }

    /** Helper: build a ClusterStorageInfo with the given byte total and 10 partitions. */
    private static ClusterStorageInfo infoWithBytes(long bytes) {
        return ClusterStorageInfo.builder()
            .clusterId("test-cluster")
            .brokerCount(3)
            .topicCount(5)
            .partitionCount(10)
            .totalLogBytes(bytes)
            .topicBytes(Collections.emptyMap())
            .build();
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    @Test
    public void zeroDataProducesZeroStorageCost() {
        // Given a cluster with no data
        ClusterStorageInfo info = infoWithBytes(0L);
        CostEstimatorConfig config = defaultConfig();

        // When costs are computed
        CostCalculator.CostReport report = new CostCalculator().calculate(info, config);

        // Then storage cost must be zero; only request costs remain
        assertEquals(0.0, report.storageCostUsd, DELTA);
        assertEquals(0.0, report.totalGib, DELTA);
        assertEquals(0.0, report.kafkaEbsGib, DELTA);
        assertEquals(0.0, report.kafkaEbsCostUsd, DELTA);
    }

    @Test
    public void zeroDataStillIncursRequestCosts() {
        // Given a cluster with 10 partitions but no persisted data
        ClusterStorageInfo info = infoWithBytes(0L);

        // When costs are computed
        CostCalculator.CostReport report = new CostCalculator().calculate(info, defaultConfig());

        // Then PUT and GET costs are non-zero because partitions still generate API calls
        assertTrue(report.putCostUsd > 0.0,
            "PUT cost should be non-zero even with 0 data bytes");
        assertTrue(report.getCostUsd > 0.0,
            "GET cost should be non-zero even with 0 data bytes");
    }

    // -----------------------------------------------------------------------
    // Known-value computations
    // -----------------------------------------------------------------------

    @Test
    public void storageCalculationMatchesExpectedValue() {
        // Given exactly 1 GiB of data
        long oneGiB = 1024L * 1024L * 1024L;
        ClusterStorageInfo info = infoWithBytes(oneGiB);
        CostEstimatorConfig config = defaultConfig(); // $0.023/GiB

        CostCalculator.CostReport report = new CostCalculator().calculate(info, config);

        // Then storage cost = 1 GiB × $0.023 = $0.023
        assertEquals(1.0, report.totalGib, DELTA);
        assertEquals(0.023, report.storageCostUsd, DELTA);
    }

    @Test
    public void putCostCalculationMatchesExpectedValue() {
        // Given 10 partitions, 1440 PUTs/partition/day, 30 days, $0.000005/req
        // Expected: 10 × 1440 × 30 × 0.000005 = $2.16
        ClusterStorageInfo info = infoWithBytes(0L); // storage cost is 0; only PUTs matter
        CostEstimatorConfig config = defaultConfig();

        CostCalculator.CostReport report = new CostCalculator().calculate(info, config);

        double expected = 10 * 1440 * 30 * 0.000005;
        assertEquals(expected, report.putCostUsd, DELTA);
    }

    @Test
    public void getCostCalculationMatchesExpectedValue() {
        // Given 10 partitions, 7200 GETs/partition/day, 30 days, $0.0000004/req
        // Expected: 10 × 7200 × 30 × 0.0000004 = $0.864
        ClusterStorageInfo info = infoWithBytes(0L);
        CostEstimatorConfig config = defaultConfig();

        CostCalculator.CostReport report = new CostCalculator().calculate(info, config);

        double expected = 10 * 7200 * 30 * 0.0000004;
        assertEquals(expected, report.getCostUsd, DELTA);
    }

    @Test
    public void kafkaEbsCostReflectsReplicationAndOverprovision() {
        // Given 10 GiB of data, RF=3, 2x over-provision → 60 GiB EBS × $0.08 = $4.80
        long tenGiB = 10L * 1024L * 1024L * 1024L;
        ClusterStorageInfo info = infoWithBytes(tenGiB);
        CostEstimatorConfig config = defaultConfig(); // RF=3, factor=2.0, $0.08/GiB

        CostCalculator.CostReport report = new CostCalculator().calculate(info, config);

        assertEquals(60.0, report.kafkaEbsGib, DELTA);
        assertEquals(4.80, report.kafkaEbsCostUsd, DELTA);
    }

    // -----------------------------------------------------------------------
    // Savings calculation
    // -----------------------------------------------------------------------

    @Test
    public void savingsArePositiveWhenAutomqIsCheaper() {
        long tenGiB = 10L * 1024L * 1024L * 1024L;
        ClusterStorageInfo info = infoWithBytes(tenGiB);

        CostCalculator.CostReport report = new CostCalculator().calculate(info, defaultConfig());

        assertTrue(report.savingsUsd >= 0.0, "Savings must be non-negative");
        assertTrue(report.savingsPct >= 0.0 && report.savingsPct <= 100.0,
            "Savings percentage must be in [0, 100]");
    }

    @Test
    public void savingsPercentageIsZeroWhenKafkaCostIsZero() {
        // Given zero data and a config with zero EBS price
        ClusterStorageInfo info = infoWithBytes(0L);
        CostEstimatorConfig config = CostEstimatorConfig.builder("localhost:9092")
            .ebsPricePerGib(0.0)
            .build();

        CostCalculator.CostReport report = new CostCalculator().calculate(info, config);

        assertEquals(0.0, report.savingsPct, DELTA,
            "Savings percentage must be 0 when Kafka EBS cost is 0");
    }

    // -----------------------------------------------------------------------
    // Replication factor variations
    // -----------------------------------------------------------------------

    @Test
    public void higherReplicationFactorIncreasesKafkaCost() {
        long oneGiB = 1024L * 1024L * 1024L;
        ClusterStorageInfo info = infoWithBytes(oneGiB);

        CostEstimatorConfig rf3 = CostEstimatorConfig.builder("localhost:9092")
            .kafkaReplicationFactor(3).overprovisionFactor(1.0).build();
        CostEstimatorConfig rf5 = CostEstimatorConfig.builder("localhost:9092")
            .kafkaReplicationFactor(5).overprovisionFactor(1.0).build();

        CostCalculator.CostReport reportRf3 = new CostCalculator().calculate(info, rf3);
        CostCalculator.CostReport reportRf5 = new CostCalculator().calculate(info, rf5);

        assertTrue(reportRf5.kafkaEbsCostUsd > reportRf3.kafkaEbsCostUsd,
            "Higher RF should result in higher Kafka EBS cost");
    }

    // -----------------------------------------------------------------------
    // automqTotal is sum of parts
    // -----------------------------------------------------------------------

    @Test
    public void automqTotalIsExactSumOfComponents() {
        long twoGiB = 2L * 1024L * 1024L * 1024L;
        ClusterStorageInfo info = infoWithBytes(twoGiB);

        CostCalculator.CostReport report = new CostCalculator().calculate(info, defaultConfig());

        double expectedTotal = report.storageCostUsd + report.putCostUsd + report.getCostUsd;
        assertEquals(expectedTotal, report.automqTotalUsd, DELTA);
    }
}
