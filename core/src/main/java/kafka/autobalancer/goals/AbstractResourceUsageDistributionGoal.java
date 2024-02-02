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

package kafka.autobalancer.goals;

import kafka.autobalancer.common.Resource;
import kafka.autobalancer.model.BrokerUpdater;

import java.util.Comparator;
import java.util.Set;

public abstract class AbstractResourceUsageDistributionGoal extends AbstractResourceDistributionGoal {
    private static final double DEFAULT_MAX_LOAD_BYTES = 100 * 1024 * 1024;
    private final Comparator<BrokerUpdater.Broker> highLoadComparator = Comparator.comparingDouble(b -> -b.load(resource()));
    private final Comparator<BrokerUpdater.Broker> lowLoadComparator = Comparator.comparingDouble(b -> b.load(resource()));

    protected long usageDetectThreshold;

    protected double usageAvgDeviation;
    private double usageAvg;
    private double usageDistLowerBound;
    private double usageDistUpperBound;

    @Override
    public void initialize(Set<BrokerUpdater.Broker> brokers) {
        Resource resource = resource();
        usageAvg = brokers.stream().mapToDouble(e -> e.load(resource)).sum() / brokers.size();
        usageDistLowerBound = Math.max(0, usageAvg * (1 - this.usageAvgDeviation));
        usageDistUpperBound = usageAvg * (1 + this.usageAvgDeviation);
    }

    @Override
    protected boolean requireLessLoad(BrokerUpdater.Broker broker) {
        return broker.load(resource()) > usageDistUpperBound;
    }

    @Override
    protected boolean requireMoreLoad(BrokerUpdater.Broker broker) {
        return broker.load(resource()) < usageDistLowerBound;
    }

    @Override
    public boolean isBrokerAcceptable(BrokerUpdater.Broker broker) {
        double load = broker.load(resource());
        if (load < this.usageDetectThreshold) {
            return true;
        }
        return load >= usageDistLowerBound && load <= usageDistUpperBound;
    }

    @Override
    public double brokerScore(BrokerUpdater.Broker broker) {
        double loadAvgDeviationAbs = Math.abs(usageAvg - broker.load(resource()));
        return GoalUtils.linearNormalization(loadAvgDeviationAbs, DEFAULT_MAX_LOAD_BYTES, 0, true);
    }

    @Override
    protected Comparator<BrokerUpdater.Broker> highLoadComparator() {
        return highLoadComparator;
    }

    @Override
    protected Comparator<BrokerUpdater.Broker> lowLoadComparator() {
        return lowLoadComparator;
    }
}
