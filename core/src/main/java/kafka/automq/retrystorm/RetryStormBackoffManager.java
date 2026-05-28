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

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

import kafka.server.retrystorm.RetryStormBackoffPolicy;
import kafka.server.retrystorm.RetryStormDelayedResponseScheduler;
import kafka.server.retrystorm.RetryStormResponseGate;

import java.util.Map;
import java.util.Set;

/**
 * Owns retry storm backoff runtime components for a broker.
 *
 * <p>The manager connects Kafka dynamic config callbacks to the shared runtime config,
 * exposes the response gate to request handlers, and closes delayed response scheduling
 * during broker shutdown. It does not evaluate responses itself.</p>
 */
public class RetryStormBackoffManager implements Reconfigurable {

    private final RetryStormBackoffConfig config;
    private final RetryStormBackoffPolicy policy;
    private final RetryStormResponseGate responseGate;
    private final RetryStormDelayedResponseScheduler scheduler;

    /**
     * Creates a manager with only config ownership, primarily for dynamic config tests.
     */
    public RetryStormBackoffManager(RetryStormBackoffConfig config) {
        this(config, null, null, null);
    }

    /**
     * Creates a broker runtime manager over config, policy, response gate, and scheduler.
     */
    public RetryStormBackoffManager(RetryStormBackoffConfig config,
                                    RetryStormBackoffPolicy policy,
                                    RetryStormResponseGate responseGate,
                                    RetryStormDelayedResponseScheduler scheduler) {
        this.config = config;
        this.policy = policy;
        this.responseGate = responseGate;
        this.scheduler = scheduler;
    }

    /**
     * Returns the broker config keys this manager can validate and apply dynamically.
     */
    @Override
    public Set<String> reconfigurableConfigs() {
        return RetryStormBackoffConfig.RECONFIGURABLE_CONFIGS;
    }

    /**
     * Validates a dynamic broker config update before Kafka publishes it to the runtime config.
     */
    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        RetryStormBackoffConfig.validate(configs);
    }

    /**
     * Applies a validated dynamic broker config update to later retry storm policy decisions.
     */
    @Override
    public void reconfigure(Map<String, ?> configs) {
        config.update(configs);
    }

    /**
     * Kafka Reconfigurable lifecycle hook; retry storm config is initialized by BrokerServer.
     */
    @Override
    public void configure(Map<String, ?> configs) {
    }

    /**
     * Closes delayed response scheduling and best-effort sends pending delayed responses.
     */
    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    /**
     * Returns the shared runtime config snapshot used by policy evaluation.
     */
    public RetryStormBackoffConfig config() {
        return config;
    }

    /**
     * Returns the policy owned by this broker, or null in config-only tests.
     */
    public RetryStormBackoffPolicy policy() {
        return policy;
    }

    /**
     * Returns the response gate injected into request send paths, or null in config-only tests.
     */
    public RetryStormResponseGate responseGate() {
        return responseGate;
    }
}
