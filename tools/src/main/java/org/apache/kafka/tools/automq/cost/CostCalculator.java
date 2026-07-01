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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pure cost calculation logic for the AutoMQ S3 Cost Estimator.
 * <p>
 * This class is stateless and has no I/O dependencies, making it straightforward
 * to unit-test in isolation.
 * <p>
 * Cost model:
 * <ul>
 *   <li><b>S3 storage</b>: {@code totalGiB × storagePricePerGib}</li>
 *   <li><b>S3 PUTs</b>: {@code partitions × putsPerPartitionPerDay × 30 × putPricePerRequest}</li>
 *   <li><b>S3 GETs</b>: {@code partitions × getsPerPartitionPerDay × 30 × getPricePerRequest}</li>
 *   <li><b>Kafka EBS equivalent</b>: {@code ebsGiB × ebsPricePerGib}, where
 *       {@code ebsGiB = totalGiB × replicationFactor × overprovisionFactor}</li>
 * </ul>
 */
public class CostCalculator {

    private static final double BYTES_PER_GIB = 1024.0 * 1024.0 * 1024.0;
    private static final int DAYS_PER_MONTH = 30;

    /**
     * Immutable result produced by {@link #calculate}.
     * All monetary values are in USD per month.
     */
    public static final class CostReport {

        /** Total logical data volume in GiB. */
        public final double totalGib;

        /** Monthly cost of S3 storage in USD. */
        public final double storageCostUsd;

        /** Estimated monthly cost of S3 PUT requests in USD. */
        public final double putCostUsd;

        /** Estimated monthly cost of S3 GET requests in USD. */
        public final double getCostUsd;

        /** Total estimated monthly AutoMQ cost in USD (storage + PUT + GET). */
        public final double automqTotalUsd;

        /** EBS storage volume (in GiB) required for an equivalent Kafka cluster. */
        public final double kafkaEbsGib;

        /** Monthly EBS cost for the equivalent Kafka cluster in USD. */
        public final double kafkaEbsCostUsd;

        /** Monthly savings of AutoMQ vs Kafka in USD. */
        public final double savingsUsd;

        /** Savings as a percentage of the Kafka EBS cost. */
        public final double savingsPct;

        /**
         * Per-topic cost breakdown in USD per month.
         * Key: topic name. Value: estimated monthly storage cost.
         * Empty when no per-topic data was provided.
         */
        public final Map<String, Double> perTopicStorageCostUsd;

        CostReport(double totalGib, double storageCostUsd, double putCostUsd, double getCostUsd,
            double kafkaEbsGib, double kafkaEbsCostUsd, Map<String, Double> perTopicStorageCostUsd) {
            this.totalGib = totalGib;
            this.storageCostUsd = storageCostUsd;
            this.putCostUsd = putCostUsd;
            this.getCostUsd = getCostUsd;
            this.automqTotalUsd = storageCostUsd + putCostUsd + getCostUsd;
            this.kafkaEbsGib = kafkaEbsGib;
            this.kafkaEbsCostUsd = kafkaEbsCostUsd;
            this.savingsUsd = Math.max(0.0, kafkaEbsCostUsd - this.automqTotalUsd);
            this.savingsPct = kafkaEbsCostUsd > 0
                ? (this.savingsUsd / kafkaEbsCostUsd) * 100.0
                : 0.0;
            this.perTopicStorageCostUsd = Collections.unmodifiableMap(perTopicStorageCostUsd);
        }
    }

    /**
     * Computes the monthly cost estimate for an AutoMQ cluster and the equivalent
     * Apache Kafka (EBS-backed) cluster.
     *
     * @param info   cluster storage facts collected from the Admin API
     * @param config pricing parameters and Kafka comparison factors
     * @return an immutable {@link CostReport} containing all computed values
     */
    public CostReport calculate(ClusterStorageInfo info, CostEstimatorConfig config) {
        double totalGib = info.totalLogBytes / BYTES_PER_GIB;

        // S3 storage cost
        double storageCost = totalGib * config.storagePricePerGib;

        // S3 PUT cost: each partition uploads WAL objects periodically
        double totalPuts = (double) info.partitionCount
            * config.estimatedPutsPerPartitionPerDay
            * DAYS_PER_MONTH;
        double putCost = totalPuts * config.putPricePerRequest;

        // S3 GET cost: consumers fetch from S3
        double totalGets = (double) info.partitionCount
            * config.estimatedGetsPerPartitionPerDay
            * DAYS_PER_MONTH;
        double getCost = totalGets * config.getPricePerRequest;

        // Kafka EBS equivalent: replicated + over-provisioned
        double kafkaEbsGib = totalGib * config.kafkaReplicationFactor * config.overprovisionFactor;
        double kafkaEbsCost = kafkaEbsGib * config.ebsPricePerGib;

        // Per-topic storage breakdown
        Map<String, Double> perTopicCosts = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : info.topicBytes.entrySet()) {
            double topicGib = entry.getValue() / BYTES_PER_GIB;
            perTopicCosts.put(entry.getKey(), topicGib * config.storagePricePerGib);
        }

        return new CostReport(totalGib, storageCost, putCost, getCost,
            kafkaEbsGib, kafkaEbsCost, perTopicCosts);
    }
}
