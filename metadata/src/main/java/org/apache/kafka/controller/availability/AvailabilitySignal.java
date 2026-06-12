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

package org.apache.kafka.controller.availability;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AvailabilitySignal {
    private AvailabilitySignalType type;
    private AvailabilityTarget target;
    private Map<String, String> attributes = new HashMap<>();

    public AvailabilitySignal() {
    }

    public AvailabilitySignal(AvailabilitySignalType type, AvailabilityTarget target) {
        this.type = type;
        this.target = target;
    }

    public AvailabilitySignalType getType() {
        return type;
    }

    public void setType(AvailabilitySignalType type) {
        this.type = type;
    }

    public AvailabilityTarget getTarget() {
        return target;
    }

    public void setTarget(AvailabilityTarget target) {
        this.target = target;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AvailabilitySignal)) {
            return false;
        }
        AvailabilitySignal that = (AvailabilitySignal) o;
        return type == that.type && Objects.equals(target, that.target) && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, target, attributes);
    }
}
