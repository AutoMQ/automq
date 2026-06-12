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

import java.util.Objects;

public class FaultAttribution {
    private final FaultAttributionType type;
    private final AvailabilityTarget target;
    private final AvailabilitySignalType sourceSignalType;
    private final String reason;

    public FaultAttribution(FaultAttributionType type, AvailabilityTarget target, AvailabilitySignalType sourceSignalType,
                            String reason) {
        this.type = type;
        this.target = target;
        this.sourceSignalType = sourceSignalType;
        this.reason = reason;
    }

    public static FaultAttribution none() {
        return new FaultAttribution(FaultAttributionType.NONE, AvailabilityTarget.cluster(), null, "no availability signal");
    }

    public FaultAttributionType type() {
        return type;
    }

    public AvailabilityTarget target() {
        return target;
    }

    public AvailabilitySignalType sourceSignalType() {
        return sourceSignalType;
    }

    public String reason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FaultAttribution)) {
            return false;
        }
        FaultAttribution that = (FaultAttribution) o;
        return type == that.type &&
            Objects.equals(target, that.target) &&
            sourceSignalType == that.sourceSignalType &&
            Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, target, sourceSignalType, reason);
    }
}
