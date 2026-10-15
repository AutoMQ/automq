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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigResource;

import java.util.Map;


public interface ConfigurationValidator {
    ConfigurationValidator NO_OP = new ConfigurationValidator() {
        @Override
        public void validate(ConfigResource resource) { }

        @Override
        public void validate(ConfigResource resource, Map<String, String> config) { }
    };

    /**
     * Throws an ApiException if the given resource is invalid to describe.
     *
     * @param resource      The configuration resource.
     */
    void validate(ConfigResource resource);

    /**
     * Throws an ApiException if a configuration is invalid for the given resource.
     *
     * @param resource      The configuration resource.
     * @param config        The new configuration.
     */
    void validate(ConfigResource resource, Map<String, String> config);

    // AutoMQ inject start
    /**
     * Throws an ApiException, or a ConfigException that the caller turns into an INVALID_CONFIG error, if the
     * alterations an AlterConfigs or IncrementalAlterConfigs request asks for are invalid for the given resource.
     * Unlike {@link #validate(ConfigResource, Map)} this is only called for such requests, never when the
     * resulting records are replayed, so it may reject a value that is already persisted and that the replay path
     * has to keep accepting.
     *
     * @param resource        The configuration resource.
     * @param alteredConfigs  Each explicitly altered key mapped to its new value, null for a deletion.
     * @param existingConfigs The configuration persisted for the resource before the request.
     */
    default void validateAlteredConfigs(ConfigResource resource, Map<String, String> alteredConfigs,
                                        Map<String, String> existingConfigs) { }
    // AutoMQ inject end
}
