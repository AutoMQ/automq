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

import java.util.List;
import java.util.OptionalLong;

/**
 * API-independent response summary consumed by retry storm policy evaluation.
 */
public class ResponseSummary {
    private final List<ResourceResult> resources;
    private final OptionalLong delayCapMs;

    /**
     * Creates a response summary without a response-specific delay cap.
     */
    public ResponseSummary(List<ResourceResult> resources) {
        this(resources, OptionalLong.empty());
    }

    /**
     * Creates a response summary with an optional response-specific delay cap.
     */
    public ResponseSummary(List<ResourceResult> resources, OptionalLong delayCapMs) {
        this.resources = List.copyOf(resources);
        this.delayCapMs = delayCapMs;
    }

    /**
     * Returns the resources whose validity and errors are evaluated by the policy.
     */
    public List<ResourceResult> resources() {
        return resources;
    }

    /**
     * Returns an optional upper bound for response delay, such as remaining Fetch max-wait budget.
     */
    public OptionalLong delayCapMs() {
        return delayCapMs;
    }
}
