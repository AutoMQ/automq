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

package kafka.autobalancer.metricsreporter.metric;

/**
 * An enum that list all the implementations of the interface. This id will be store in the serialized
 * metrics to help the metric sampler to decide using which class to deserialize the metric bytes.
 */
public enum MetricClassId {
    PARTITION_METRIC((byte) 0);

    private final byte id;

    MetricClassId(byte id) {
        this.id = id;
    }

    static MetricClassId forId(byte id) {
        if (id < values().length) {
            return values()[id];
        } else {
            throw new IllegalArgumentException("MetricClassId " + id + " does not exist.");
        }
    }

    byte id() {
        return id;
    }
}
