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

/**
 * API-independent result for one resource represented in a Kafka response.
 */
public class ResourceResult {
    private final String resourceKey;
    private final boolean valid;
    private final boolean delayableTransient;
    private final boolean protective;

    /**
     * Creates one policy-neutral resource classification.
     */
    public ResourceResult(String resourceKey, boolean valid, boolean delayableTransient, boolean protective) {
        this.resourceKey = resourceKey;
        this.valid = valid;
        this.delayableTransient = delayableTransient;
        this.protective = protective;
    }

    /**
     * Returns the per-client resource key used by retry storm backoff state.
     */
    public String resourceKey() {
        return resourceKey;
    }

    /**
     * Returns whether this resource carries usable response data and should clear backoff state.
     */
    public boolean valid() {
        return valid;
    }

    /**
     * Returns whether this resource failed with a transient error that is eligible for fast backoff.
     */
    public boolean delayableTransient() {
        return delayableTransient;
    }

    /**
     * Returns whether this resource failed with any protective error that can count toward backoff.
     */
    public boolean protective() {
        return protective;
    }
}
