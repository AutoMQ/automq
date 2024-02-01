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
