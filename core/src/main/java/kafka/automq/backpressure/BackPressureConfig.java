/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.automq.backpressure;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BackPressureConfig {

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
        AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG,
        AutoMQConfig.S3_BACK_PRESSURE_COOLDOWN_MS_CONFIG
    );

    private volatile boolean enabled;
    /**
     * The cooldown time in milliseconds to wait between two regulator actions.
     */
    private long cooldownMs;

    public static BackPressureConfig from(KafkaConfig config) {
        return new BackPressureConfig(config.s3BackPressureEnabled(), config.s3BackPressureCooldownMs());
    }

    public static BackPressureConfig from(Map<String, ?> raw) {
        Map<String, Object> configs = new HashMap<>(raw);
        return new BackPressureConfig(
            ConfigUtils.getBoolean(configs, AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG),
            ConfigUtils.getLong(configs, AutoMQConfig.S3_BACK_PRESSURE_COOLDOWN_MS_CONFIG)
        );
    }

    public BackPressureConfig(boolean enabled, long cooldownMs) {
        this.enabled = enabled;
        this.cooldownMs = cooldownMs;
    }

    public static void validate(Map<String, ?> raw) throws ConfigException {
        Map<String, Object> configs = new HashMap<>(raw);
        if (configs.containsKey(AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG)) {
            ConfigUtils.getBoolean(configs, AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG);
        }
        if (configs.containsKey(AutoMQConfig.S3_BACK_PRESSURE_COOLDOWN_MS_CONFIG)) {
            validateCooldownMs(ConfigUtils.getLong(configs, AutoMQConfig.S3_BACK_PRESSURE_COOLDOWN_MS_CONFIG));
        }
    }

    public static void validateCooldownMs(long cooldownMs) throws ConfigException {
        if (cooldownMs < 0) {
            throw new ConfigException(AutoMQConfig.S3_BACK_PRESSURE_COOLDOWN_MS_CONFIG, cooldownMs, "The cooldown time must be non-negative.");
        }
    }

    public void update(Map<String, ?> raw) {
        Map<String, Object> configs = new HashMap<>(raw);
        if (configs.containsKey(AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG)) {
            this.enabled = ConfigUtils.getBoolean(configs, AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG);
        }
        if (configs.containsKey(AutoMQConfig.S3_BACK_PRESSURE_COOLDOWN_MS_CONFIG)) {
            this.cooldownMs = ConfigUtils.getLong(configs, AutoMQConfig.S3_BACK_PRESSURE_COOLDOWN_MS_CONFIG);
        }
    }

    public boolean enabled() {
        return enabled;
    }

    public long cooldownMs() {
        return cooldownMs;
    }
}
