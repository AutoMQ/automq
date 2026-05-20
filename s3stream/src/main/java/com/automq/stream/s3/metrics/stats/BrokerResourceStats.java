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

package com.automq.stream.s3.metrics.stats;

import java.util.Objects;
import java.util.function.DoubleSupplier;

public class BrokerResourceStats {
    private static final DoubleSupplier UNAVAILABLE = () -> Double.NaN;
    private static final BrokerResourceStats INSTANCE = new BrokerResourceStats();

    private volatile DoubleSupplier cpuUtilSupplier = UNAVAILABLE;
    private volatile DoubleSupplier networkUtilSupplier = UNAVAILABLE;

    private BrokerResourceStats() {
    }

    public static BrokerResourceStats getInstance() {
        return INSTANCE;
    }

    public void setCpuUtilSupplier(DoubleSupplier cpuUtilSupplier) {
        this.cpuUtilSupplier = Objects.requireNonNull(cpuUtilSupplier, "cpuUtilSupplier");
    }

    public void setNetworkUtilSupplier(DoubleSupplier networkUtilSupplier) {
        this.networkUtilSupplier = Objects.requireNonNull(networkUtilSupplier, "networkUtilSupplier");
    }

    public double cpuUtil() {
        return getAsDouble(cpuUtilSupplier);
    }

    public double networkUtil() {
        return getAsDouble(networkUtilSupplier);
    }

    public void reset() {
        cpuUtilSupplier = UNAVAILABLE;
        networkUtilSupplier = UNAVAILABLE;
    }

    private double getAsDouble(DoubleSupplier supplier) {
        try {
            return supplier.getAsDouble();
        } catch (Throwable ignored) {
            return Double.NaN;
        }
    }
}
