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

import kafka.autobalancer.common.Resource;

import java.util.Objects;

public class Broker extends RawMetricInstance {
    private final int brokerId;
    private boolean active;

    public Broker(int brokerId, boolean active) {
        this.brokerId = brokerId;
        this.active = active;
    }

    public Broker(Broker other) {
        this.brokerId = other.brokerId;
        this.active = other.active;
        clone(other);
    }

    public int getBrokerId() {
        return this.brokerId;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isActive() {
        return this.active;
    }

    public void reduceLoad(Resource resource, double delta) {
        this.setLoad(resource, load(resource) - delta);
    }

    public void addLoad(Resource resource, double delta) {
        this.setLoad(resource, load(resource) + delta);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Broker broker = (Broker) o;
        return brokerId == broker.brokerId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(brokerId);
    }

    public String shortString() {
        return "Broker{" +
                "brokerId=" + brokerId +
                ", active=" + active +
                ", " + timeString() +
                ", " + loadString() +
                "}";
    }

    @Override
    public String toString() {
        return "Broker{" +
                "brokerId=" + brokerId +
                ", active=" + active +
                ", " + super.toString() +
                "}";
    }
}
