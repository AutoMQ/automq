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
import kafka.autobalancer.model.Broker;

import java.util.Comparator;
import java.util.Set;

public abstract class AbstractLoadDistributionGoal extends AbstractResourceDistributionGoal {
    private static final double DEFAULT_MAX_LOAD_BYTES = 100 * 1024 * 1024;
    private final Comparator<Broker> highLoadComparator = Comparator.comparingDouble(b -> -b.load(resource()));
    private final Comparator<Broker> lowLoadComparator = Comparator.comparingDouble(b -> b.load(resource()));

    protected long loadDetectThreshold;

    protected double loadAvgDeviation;
    private double loadAvg;
    private double loadDistLowerBound;
    private double loadDistUpperBound;

    @Override
    void calcUsageBound(Set<Broker> brokers) {
        Resource resource = resource();
        loadAvg = brokers.stream().mapToDouble(e -> e.load(resource)).sum() / brokers.size();
        loadDistLowerBound = Math.max(0, loadAvg * (1 - this.loadAvgDeviation));
        loadDistUpperBound = loadAvg * (1 + this.loadAvgDeviation);
    }

    @Override
    boolean requireLessLoad(double load) {
        return load > loadDistUpperBound;
    }

    @Override
    boolean requireMoreLoad(double load) {
        return load < loadDistLowerBound;
    }

    @Override
    public boolean isBrokerAcceptable(Broker broker) {
        double load = broker.load(resource());
        if (load < this.loadDetectThreshold) {
            return true;
        }
        return !requireLessLoad(load) && !requireMoreLoad(load);
    }

    @Override
    public double brokerScore(Broker broker) {
        double loadAvgDeviationAbs = Math.abs(loadAvg - broker.load(resource()));
        return GoalUtils.linearNormalization(loadAvgDeviationAbs, DEFAULT_MAX_LOAD_BYTES, 0, true);
    }

    @Override
    protected Comparator<Broker> highLoadComparator() {
        return highLoadComparator;
    }

    @Override
    protected Comparator<Broker> lowLoadComparator() {
        return lowLoadComparator;
    }
}
