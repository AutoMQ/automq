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

package com.automq.log.uploader.selector.runtime;

import com.automq.log.uploader.selector.LogUploaderNodeSelectorProvider;

/**
 * Selector that follows the Kafka Connect distributed herder leader election state.
 */
public class ConnectLeaderSelectorProvider extends AbstractRuntimeLeaderSelectorProvider implements LogUploaderNodeSelectorProvider {
    public static final String TYPE = "connect-leader";
    static final String REGISTRY_KEY = "connect-leader";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected String registryKey() {
        return REGISTRY_KEY;
    }
}
