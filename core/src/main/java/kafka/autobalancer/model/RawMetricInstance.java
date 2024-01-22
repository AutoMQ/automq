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

package kafka.autobalancer.model;

import kafka.autobalancer.common.RawMetricType;
import kafka.autobalancer.common.Resource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RawMetricInstance {
    protected final double[] loads = new double[Resource.cachedValues().size()];
    protected final Set<Resource> resources = new HashSet<>();
    protected Map<RawMetricType, Double> metricsMap = new HashMap<>();
    protected long timestamp = 0L;

    protected void clone(RawMetricInstance other) {
        System.arraycopy(other.loads, 0, this.loads, 0, loads.length);
        this.resources.clear();
        this.resources.addAll(other.resources);
        this.metricsMap.clear();
        this.metricsMap.putAll(other.metricsMap);
        this.timestamp = other.timestamp;
    }

    public void setLoad(Resource resource, double value) {
        this.resources.add(resource);
        this.loads[resource.id()] = value;
    }

    public double load(Resource resource) {
        if (!this.resources.contains(resource)) {
            return 0.0;
        }
        return this.loads[resource.id()];
    }

    public void update(Map<RawMetricType, Double> metricsMap, long timestamp) {
        this.metricsMap = metricsMap;
        this.timestamp = timestamp;
    }

    public Map<RawMetricType, Double> getMetricsMap() {
        return this.metricsMap;
    }

    public double ofValue(RawMetricType metricType) {
        return this.metricsMap.getOrDefault(metricType, 0.0);
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    protected String timeString() {
        return "timestamp=" + timestamp;
    }

    protected String loadString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Loads={");
        for (int i = 0; i < loads.length; i++) {
            builder.append(Resource.of(i).resourceString(loads[i]));
            if (i != loads.length - 1) {
                builder.append(", ");
            }
        }
        builder.append("}");
        return builder.toString();
    }

    protected String metricsString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Metrics={");
        int i = 0;
        for (Map.Entry<RawMetricType, Double> entry : metricsMap.entrySet()) {
            builder.append(entry.getKey())
                    .append("=")
                    .append(entry.getValue());
            if (i != metricsMap.size() - 1) {
                builder.append(", ");
            }
            i++;
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public String toString() {
        return timeString() +
                ", " +
                loadString() +
                ", " +
                metricsString();
    }
}
