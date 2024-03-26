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
import kafka.autobalancer.model.BrokerUpdater;
import kafka.server.KafkaConfig;
import org.slf4j.Logger;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public abstract class AbstractResourceUsageDistributionGoal extends AbstractResourceDistributionGoal {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    private final Comparator<BrokerUpdater.Broker> highLoadComparator = Comparator.comparingDouble(b -> -b.load(resource()));
    private final Comparator<BrokerUpdater.Broker> lowLoadComparator = Comparator.comparingDouble(b -> b.load(resource()));

    protected long maxNormalizedLoadBytes = 100 * 1024 * 1024;
    protected long usageDetectThreshold;
    protected double usageAvgDeviation;
    protected double usageAvg;
    protected double usageDistLowerBound;
    protected double usageDistUpperBound;

    @Override
    public void initialize(Set<BrokerUpdater.Broker> brokers) {
        Resource resource = resource();
        usageAvg = brokers.stream().mapToDouble(e -> e.load(resource)).sum() / brokers.size();
        usageDistLowerBound = Math.max(0, usageAvg * (1 - this.usageAvgDeviation));
        usageDistUpperBound = usageAvg * (1 + this.usageAvgDeviation);
        LOGGER.info("{} expected dist bound: {}", name(), String.format("%.2f-%f.2f", usageDistLowerBound / 1024, usageDistUpperBound / 1024));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(KafkaConfig.S3NetworkBaselineBandwidthProp())) {
            Object nwBandwidth = configs.get(KafkaConfig.S3NetworkBaselineBandwidthProp());
            try {
                if (nwBandwidth instanceof Long) {
                    this.maxNormalizedLoadBytes = (Long) nwBandwidth;
                } else if (nwBandwidth instanceof Integer) {
                    this.maxNormalizedLoadBytes = (Integer) nwBandwidth;
                } else if (nwBandwidth instanceof String) {
                    this.maxNormalizedLoadBytes = Long.parseLong((String) nwBandwidth);
                } else {
                    LOGGER.error("Failed to parse max normalized load bytes from config {}, using default value", nwBandwidth);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to parse max normalized load bytes from config {}, using default value", nwBandwidth, e);
            }
        }
        LOGGER.info("{} using maxNormalizedLoadBytes: {}", name(), this.maxNormalizedLoadBytes);
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
        return GoalUtils.linearNormalization(loadAvgDeviationAbs, maxNormalizedLoadBytes, 0, true);
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
