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

import kafka.autobalancer.common.Resource;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NetworkOutDistributionGoal extends AbstractResourceDistributionGoal {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkOutDistributionGoal.class);

    @Override
    public String name() {
        return NetworkOutDistributionGoal.class.getSimpleName();
    }

    @Override
    protected Resource resource() {
        return Resource.NW_OUT;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(configs, false);
        this.utilizationDetectThreshold = controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD);
        this.utilizationAvgDeviation = controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION);
    }

    @Override
    public void onBalanceFailed(BrokerUpdater.Broker broker) {
        LOGGER.warn("Failed to balance broker {} network outbound load after iterating all partitions", broker.getBrokerId());
    }

    @Override
    public int priority() {
        return GoalConstants.NETWORK_DISTRIBUTION_GOAL_PRIORITY;
    }
}
