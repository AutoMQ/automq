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
/*
 * Some portion of this fil Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.autobalancer.common;

import java.util.List;


/**
 * CPU: a host and broker-level resource.
 * NW (in and out): a host-level resource.
 * DISK: a broker-level resource.
 */
public enum Resource {
    CPU("cpu", 0, 0.001),
    NW_IN("networkInbound", 1, 10),
    NW_OUT("networkOutbound", 2, 10);

    // EPSILON_PERCENT defines the acceptable nuance when comparing the utilization of the resource.
    // This nuance is generated due to precision loss when summing up float type utilization value.
    // In stress test we find that for cluster of around 800,000 replicas, the summed up nuance can be
    // more than 0.1% of sum value.
    private static final double EPSILON_PERCENT = 0.0008;
    private static final List<Resource> CACHED_VALUES = List.of(values());
    private final String resource;
    private final int id;
    private final double epsilon;

    Resource(String resource, int id, double epsilon) {
        this.resource = resource;
        this.id = id;
        this.epsilon = epsilon;
    }

    /**
     * Use this instead of values() because values() creates a new array each time.
     *
     * @return enumerated values in the same order as values()
     */
    public static List<Resource> cachedValues() {
        return CACHED_VALUES;
    }

    /**
     * @return The resource type.
     */
    public String resource() {
        return resource;
    }

    /**
     * @return The resource id.
     */
    public int id() {
        return id;
    }

    /**
     * The epsilon value used in comparing the given values.
     *
     * @param value1 The first value used in comparison.
     * @param value2 The second value used in comparison.
     * @return The epsilon value used in comparing the given values.
     */
    public double epsilon(double value1, double value2) {
        return Math.max(epsilon, EPSILON_PERCENT * (value1 + value2));
    }

    @Override
    public String toString() {
        return resource;
    }
}

