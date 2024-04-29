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

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.Resource;
import kafka.autobalancer.common.normalizer.Normalizer;
import kafka.autobalancer.common.normalizer.StepNormalizer;
import kafka.autobalancer.model.BrokerUpdater;
import org.slf4j.Logger;

import java.util.Comparator;
import java.util.Set;

public abstract class AbstractResourceUsageDistributionGoal extends AbstractResourceDistributionGoal {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    private final Comparator<BrokerUpdater.Broker> highLoadComparator = Comparator.comparingDouble(b -> -b.load(resource()));
    private final Comparator<BrokerUpdater.Broker> lowLoadComparator = Comparator.comparingDouble(b -> b.load(resource()));
    protected Normalizer normalizer;
    protected volatile long usageDetectThreshold;
    protected volatile double usageAvgDeviationRatio;
    protected double usageAvg;
    protected double usageAvgDeviation;
    protected double usageDistLowerBound;
    protected double usageDistUpperBound;

    @Override
    public void initialize(Set<BrokerUpdater.Broker> brokers) {
        Resource resource = resource();
        usageAvg = brokers.stream().mapToDouble(e -> e.load(resource)).sum() / brokers.size();
        usageAvgDeviation = usageAvg * usageAvgDeviationRatio;
        usageDistLowerBound = Math.max(0, usageAvg * (1 - this.usageAvgDeviationRatio));
        usageDistUpperBound = usageAvg * (1 + this.usageAvgDeviationRatio);
        normalizer = new StepNormalizer(usageAvgDeviation, usageAvgDeviation + linearNormalizerThreshold(), 0.9);
        LOGGER.info("{} expected dist bound: {}", name(), String.format("%s-%s", resource.resourceString(usageDistLowerBound),
                resource.resourceString(usageDistUpperBound)));
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
}
