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

/**
 * Immutable configuration for the S3 Cost Estimator tool.
 * <p>
 * Holds the bootstrap server address, optional Admin client property file path,
 * S3/EBS pricing parameters, and Kafka comparison factors. All prices are in
 * USD. Sizes are in GiB.
 */
public final class CostEstimatorConfig {

    /** Bootstrap server connection string for the Admin client. */
    public final String bootstrapServer;

    /** Optional path to an Admin client properties file. May be null. */
    public final String configFile;

    /**
     * S3 storage price in USD per GiB per month.
     * Default: AWS us-east-1 S3 Standard storage ($0.023/GiB).
     */
    public final double storagePricePerGib;

    /**
     * S3 PUT request price in USD per request.
     * Default: AWS us-east-1 ($0.000005 per request = $5 per million).
     */
    public final double putPricePerRequest;

    /**
     * S3 GET request price in USD per request.
     * Default: AWS us-east-1 ($0.0000004 per request = $0.40 per million).
     */
    public final double getPricePerRequest;

    /**
     * EBS gp3 price in USD per GiB per month used for the Kafka cost comparison.
     * Default: AWS us-east-1 gp3 ($0.08/GiB).
     */
    public final double ebsPricePerGib;

    /**
     * Replication factor to assume when simulating an equivalent Apache Kafka cluster.
     * Default: 3 (standard production setup).
     */
    public final int kafkaReplicationFactor;

    /**
     * Over-provisioning multiplier applied on top of the replicated storage size
     * to account for typical Kafka disk headroom requirements.
     * Default: 2.0.
     */
    public final double overprovisionFactor;

    /**
     * Estimated number of S3 PUT requests (WAL object uploads) per partition per day.
     * One WAL upload happens roughly every minute under moderate load.
     */
    public final int estimatedPutsPerPartitionPerDay;

    /**
     * Estimated number of S3 GET requests (consumer fetches) per partition per day.
     * Approximately 5 fetches per minute per partition under moderate load.
     */
    public final int estimatedGetsPerPartitionPerDay;

    /** Output format: "text" (default) or "json". */
    public final String outputFormat;

    /** Whether to print a per-topic cost breakdown. */
    public final boolean perTopic;

    private CostEstimatorConfig(Builder builder) {
        this.bootstrapServer = builder.bootstrapServer;
        this.configFile = builder.configFile;
        this.storagePricePerGib = builder.storagePricePerGib;
        this.putPricePerRequest = builder.putPricePerRequest;
        this.getPricePerRequest = builder.getPricePerRequest;
        this.ebsPricePerGib = builder.ebsPricePerGib;
        this.kafkaReplicationFactor = builder.kafkaReplicationFactor;
        this.overprovisionFactor = builder.overprovisionFactor;
        this.estimatedPutsPerPartitionPerDay = builder.estimatedPutsPerPartitionPerDay;
        this.estimatedGetsPerPartitionPerDay = builder.estimatedGetsPerPartitionPerDay;
        this.outputFormat = builder.outputFormat;
        this.perTopic = builder.perTopic;
    }

    /** Returns a new builder with AWS us-east-1 pricing defaults. */
    public static Builder builder(String bootstrapServer) {
        return new Builder(bootstrapServer);
    }

    /** Builder for {@link CostEstimatorConfig}. */
    public static final class Builder {
        private final String bootstrapServer;
        private String configFile = null;
        private double storagePricePerGib = 0.023;
        private double putPricePerRequest = 0.000005;
        private double getPricePerRequest = 0.0000004;
        private double ebsPricePerGib = 0.08;
        private int kafkaReplicationFactor = 3;
        private double overprovisionFactor = 2.0;
        private int estimatedPutsPerPartitionPerDay = 1440;   // ~1 per minute
        private int estimatedGetsPerPartitionPerDay = 7200;   // ~5 per minute
        private String outputFormat = "text";
        private boolean perTopic = false;

        private Builder(String bootstrapServer) {
            this.bootstrapServer = bootstrapServer;
        }

        public Builder configFile(String configFile) {
            this.configFile = configFile;
            return this;
        }

        public Builder storagePricePerGib(double price) {
            this.storagePricePerGib = price;
            return this;
        }

        public Builder putPricePerRequest(double price) {
            this.putPricePerRequest = price;
            return this;
        }

        public Builder getPricePerRequest(double price) {
            this.getPricePerRequest = price;
            return this;
        }

        public Builder ebsPricePerGib(double price) {
            this.ebsPricePerGib = price;
            return this;
        }

        public Builder kafkaReplicationFactor(int factor) {
            this.kafkaReplicationFactor = factor;
            return this;
        }

        public Builder overprovisionFactor(double factor) {
            this.overprovisionFactor = factor;
            return this;
        }

        public Builder estimatedPutsPerPartitionPerDay(int puts) {
            this.estimatedPutsPerPartitionPerDay = puts;
            return this;
        }

        public Builder estimatedGetsPerPartitionPerDay(int gets) {
            this.estimatedGetsPerPartitionPerDay = gets;
            return this;
        }

        public Builder outputFormat(String format) {
            this.outputFormat = format;
            return this;
        }

        public Builder perTopic(boolean perTopic) {
            this.perTopic = perTopic;
            return this;
        }

        public CostEstimatorConfig build() {
            return new CostEstimatorConfig(this);
        }
    }
}
