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
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater;
import org.slf4j.Logger;

import java.util.Map;

public class NetworkOutUsageDistributionGoal extends AbstractResourceUsageDistributionGoal {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);

    @Override
    public String name() {
        return NetworkOutUsageDistributionGoal.class.getSimpleName();
    }

    @Override
    protected Resource resource() {
        return Resource.NW_OUT;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(configs, false);
        this.usageDetectThreshold = controllerConfig.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
        this.usageAvgDeviation = Math.max(0.0, Math.min(1.0,
                controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION)));
    }

    @Override
    public void onBalanceFailed(BrokerUpdater.Broker broker) {
        LOGGER.warn("Failed to balance broker {} network outbound load after iterating all partitions", broker.getBrokerId());
    }

    @Override
    public GoalType type() {
        return GoalType.SOFT;
    }
}
