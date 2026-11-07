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

package kafka.server.retrystorm;

import org.apache.kafka.common.protocol.ApiKeys;

/**
 * Request metadata needed by the retry storm response gate.
 */
public class RetryStormRequestContext {
    private final ApiKeys apiKey;
    private final String clientScope;
    private final long nowMs;

    /**
     * Creates immutable request metadata for one response-gate invocation.
     */
    public RetryStormRequestContext(ApiKeys apiKey, String clientScope, long nowMs) {
        this.apiKey = apiKey;
        this.clientScope = clientScope;
        this.nowMs = nowMs;
    }

    /**
     * Returns the Kafka API key associated with the response.
     */
    public ApiKeys apiKey() {
        return apiKey;
    }

    /**
     * Returns the client isolation scope used for per-client retry storm state.
     */
    public String clientScope() {
        return clientScope;
    }

    /**
     * Returns the logical time for the response-gate policy evaluation.
     */
    public long nowMs() {
        return nowMs;
    }
}
