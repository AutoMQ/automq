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

/**
 * Runtime snapshot for retry storm backoff broker configs.
 *
 * <p>The object is shared by request-time policy evaluation and Kafka dynamic
 * config callbacks. Reads are lock-free through volatile fields; updates validate
 * only retry storm keys and affect later decisions without changing already
 * scheduled responses.</p>
 */
public class RetryStormBackoffConfig {

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
        AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG,
        AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG
    );

    private volatile boolean enabled;
    private volatile long maxDelayMs;

    /**
     * Creates a runtime config snapshot with explicit values.
     */
    public RetryStormBackoffConfig(boolean enabled, long maxDelayMs) {
        validateMaxDelayMs(maxDelayMs);
        this.enabled = enabled;
        this.maxDelayMs = maxDelayMs;
    }

    /**
     * Builds the initial runtime config from the broker's parsed KafkaConfig.
     */
    public static RetryStormBackoffConfig from(KafkaConfig config) {
        return new RetryStormBackoffConfig(config.retryStormBackoffEnabled(), config.retryStormBackoffMaxDelayMs());
    }

    /**
     * Builds a runtime config from raw config values and validates max delay before publishing it.
     */
    public static RetryStormBackoffConfig from(Map<String, ?> raw) {
        Map<String, Object> configs = new HashMap<>(raw);
        long maxDelayMs = ConfigUtils.getLong(configs, AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG);
        validateMaxDelayMs(maxDelayMs);
        return new RetryStormBackoffConfig(
            ConfigUtils.getBoolean(configs, AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG),
            maxDelayMs
        );
    }

    /**
     * Validates retry storm keys present in a dynamic broker config update.
     */
    public static void validate(Map<String, ?> raw) throws ConfigException {
        Map<String, Object> configs = new HashMap<>(raw);
        if (configs.containsKey(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG)) {
            ConfigUtils.getBoolean(configs, AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG);
        }
        if (configs.containsKey(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG)) {
            validateMaxDelayMs(ConfigUtils.getLong(configs, AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG));
        }
    }

    /**
     * Rejects max delay values outside the bounded delayed-response range.
     */
    public static void validateMaxDelayMs(long maxDelayMs) throws ConfigException {
        if (maxDelayMs < 0 || maxDelayMs > AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_MAX) {
            throw new ConfigException(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, maxDelayMs,
                "The retry storm backoff max delay must be between 0 and "
                    + AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_MAX + " milliseconds.");
        }
    }

    /**
     * Applies a partial dynamic update; keys absent from {@code raw} keep their current runtime values.
     */
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

    /**
     * Returns whether retry storm delayed responses are enabled for future policy evaluations.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Returns the current maximum delay in milliseconds for future policy decisions.
     */
    public long maxDelayMs() {
        return maxDelayMs;
    }
}
