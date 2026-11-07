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

package kafka.autobalancer.goals;

import kafka.autobalancer.common.normalizer.Normalizer;
import kafka.autobalancer.common.normalizer.StepNormalizer;
import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.model.BrokerUpdater;

import java.util.Collection;
import java.util.Comparator;

public abstract class AbstractResourceUsageDistributionGoal extends AbstractResourceDistributionGoal {
    private final Comparator<BrokerUpdater.Broker> highLoadComparator = Comparator.comparingDouble(b -> -b.loadValue(resource()));
    private final Comparator<BrokerUpdater.Broker> lowLoadComparator = Comparator.comparingDouble(b -> b.loadValue(resource()));
    protected Normalizer normalizer;
    protected long usageDetectThreshold;
    protected double usageAvgDeviationRatio;
    protected double usageTrivialRatio;
    protected double usageAvg;
    protected double usageAvgDeviation;
    protected double usageDistLowerBound;
    protected double usageDistUpperBound;

    @Override
    public void initialize(Collection<BrokerUpdater.Broker> brokers) {
        super.initialize(brokers);
        byte resource = resource();
        usageAvg = brokers.stream().mapToDouble(e -> e.loadValue(resource)).sum() / brokers.size();
        usageAvgDeviation = usageAvg * usageAvgDeviationRatio;
        usageDistLowerBound = Math.max(0, usageAvg * (1 - this.usageAvgDeviationRatio));
        usageDistUpperBound = usageAvg * (1 + this.usageAvgDeviationRatio);
        normalizer = new StepNormalizer(usageAvgDeviation, usageAvgDeviation + linearNormalizerThreshold(), 0.9);
        LOGGER.info("{} expected dist bound: {}", name(), String.format("%s-%s", Resource.resourceString(resource, usageDistLowerBound),
                Resource.resourceString(resource, usageDistUpperBound)));
    }

    @Override
    protected boolean requireLessLoad(BrokerUpdater.Broker broker) {
        return broker.loadValue(resource()) > usageDistUpperBound;
    }

    @Override
    protected boolean requireMoreLoad(BrokerUpdater.Broker broker) {
        return broker.loadValue(resource()) < usageDistLowerBound;
    }

    @Override
    public boolean isBrokerAcceptable(BrokerUpdater.Broker broker) {
        double load = broker.loadValue(resource());
        if (load < this.usageDetectThreshold && usageAvg < this.usageDetectThreshold) {
            return true;
        }
        return load >= usageDistLowerBound && load <= usageDistUpperBound;
    }

    @Override
    public double brokerScore(BrokerUpdater.Broker broker) {
        double loadAvgDeviationAbs = Math.abs(usageAvg - broker.loadValue(resource()));
        if (loadAvgDeviationAbs < usageAvgDeviation) {
            return 1.0;
        }
        return normalizer.normalize(loadAvgDeviationAbs, true);
    }

    @Override
    protected Comparator<BrokerUpdater.Broker> highLoadComparator() {
        return highLoadComparator;
    }

    @Override
    protected Comparator<BrokerUpdater.Broker> lowLoadComparator() {
        return lowLoadComparator;
    }

    public long getUsageDetectThreshold() {
        return usageDetectThreshold;
    }

    public double getUsageAvgDeviationRatio() {
        return usageAvgDeviationRatio;
    }

    public abstract double linearNormalizerThreshold();

    @Override
    public boolean isTrivialLoadChange(BrokerUpdater.Broker broker, double loadChange) {
        return Math.abs(loadChange) < usageTrivialRatio * usageAvg;
    }
}
