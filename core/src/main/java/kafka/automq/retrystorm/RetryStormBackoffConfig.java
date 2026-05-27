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

package kafka.automq.retrystorm;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RetryStormBackoffConfig {

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
        AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG,
        AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG
    );

    private volatile boolean enabled;
    private volatile long maxDelayMs;

    public RetryStormBackoffConfig(boolean enabled, long maxDelayMs) {
        this.enabled = enabled;
        this.maxDelayMs = maxDelayMs;
    }

    public static RetryStormBackoffConfig from(KafkaConfig config) {
        return new RetryStormBackoffConfig(config.retryStormBackoffEnabled(), config.retryStormBackoffMaxDelayMs());
    }

    public static RetryStormBackoffConfig from(Map<String, ?> raw) {
        Map<String, Object> configs = new HashMap<>(raw);
        long maxDelayMs = ConfigUtils.getLong(configs, AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG);
        validateMaxDelayMs(maxDelayMs);
        return new RetryStormBackoffConfig(
            ConfigUtils.getBoolean(configs, AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG),
            maxDelayMs
        );
    }

    public static void validate(Map<String, ?> raw) throws ConfigException {
        Map<String, Object> configs = new HashMap<>(raw);
        if (configs.containsKey(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG)) {
            ConfigUtils.getBoolean(configs, AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG);
        }
        if (configs.containsKey(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG)) {
            validateMaxDelayMs(ConfigUtils.getLong(configs, AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG));
        }
    }

    public static void validateMaxDelayMs(long maxDelayMs) throws ConfigException {
        if (maxDelayMs < 0) {
            throw new ConfigException(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, maxDelayMs,
                "The retry storm backoff max delay must be non-negative.");
        }
    }

    public void update(Map<String, ?> raw) {
        Map<String, Object> configs = new HashMap<>(raw);
        if (configs.containsKey(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG)) {
            this.enabled = ConfigUtils.getBoolean(configs, AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG);
        }
        if (configs.containsKey(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG)) {
            long nextMaxDelayMs = ConfigUtils.getLong(configs, AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG);
            validateMaxDelayMs(nextMaxDelayMs);
            this.maxDelayMs = nextMaxDelayMs;
        }
    }

    public boolean enabled() {
        return enabled;
    }

    public long maxDelayMs() {
        return maxDelayMs;
    }
}
