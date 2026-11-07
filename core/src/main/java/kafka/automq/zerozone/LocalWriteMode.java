/*
 * Copyright 2026, AutoMQ HK Limited.
 *
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

package kafka.automq.zerozone;

import java.util.Arrays;

/**
 * Controls how ZeroZone V2 persists Produce records whose target Partition is on the current Broker.
 */
public enum LocalWriteMode {
    ROUTER_CHANNEL("router_channel"),
    DIRECT("direct");

    private final String configName;

    LocalWriteMode(String configName) {
        this.configName = configName;
    }

    /**
     * Returns the stable value accepted by broker configuration.
     */
    public String configName() {
        return configName;
    }

    /**
     * Resolves a broker configuration value without requiring a specific case.
     *
     * @param configName broker configuration value
     * @return matching local write mode
     * @throws IllegalArgumentException if the value does not identify a supported mode
     */
    public static LocalWriteMode fromName(String configName) {
        return Arrays.stream(values())
            .filter(mode -> mode.configName.equalsIgnoreCase(configName))
            .findFirst()
            .orElseThrow(() ->
                new IllegalArgumentException("Unsupported ZeroZone local write mode: " + configName));
    }

    /**
     * Returns all stable values accepted by broker configuration.
     */
    public static String[] configNames() {
        return Arrays.stream(values()).map(LocalWriteMode::configName).toArray(String[]::new);
    }
}
