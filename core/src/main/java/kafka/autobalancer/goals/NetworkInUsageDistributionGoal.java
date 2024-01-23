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

package kafka.autobalancer.goals;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.Resource;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater;
import org.slf4j.Logger;

import java.util.Map;

public class NetworkInUsageDistributionGoal extends AbstractResourceUsageDistributionGoal {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);

    @Override
    public String name() {
        return NetworkInUsageDistributionGoal.class.getSimpleName();
    }

    @Override
    protected Resource resource() {
        return Resource.NW_IN;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(configs, false);
        this.usageDetectThreshold = controllerConfig.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
        this.usageAvgDeviation = Math.max(0.0, Math.min(1.0,
                controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION)));
    }

    @Override
    public void onBalanceFailed(BrokerUpdater.Broker broker) {
        LOGGER.warn("Failed to balance broker {} network inbound load after iterating all partitions", broker.getBrokerId());
    }

    @Override
    public GoalType type() {
        return GoalType.SOFT;
    }
}
