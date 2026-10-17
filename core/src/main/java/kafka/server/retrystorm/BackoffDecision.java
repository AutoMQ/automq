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
 * Response-level policy decision returned to the response gate.
 */
public class BackoffDecision {
    private static final BackoffDecision IMMEDIATE = new BackoffDecision(BackoffAction.IMMEDIATE);

    private final BackoffAction action;
    private final long delayMs;
    private final int reasonMask;

    /**
     * Creates a response decision without delayed-response reason flags.
     */
    public BackoffDecision(BackoffAction action) {
        this(action, 0L, 0);
    }

    /**
     * Creates a policy decision with the response action, selected delay, and combined reason flags.
     */
    public BackoffDecision(BackoffAction action, long delayMs, int reasonMask) {
        this.action = action;
        this.delayMs = delayMs;
        this.reasonMask = reasonMask;
    }

    /**
     * Returns the shared immediate decision instance.
     */
    public static BackoffDecision immediate() {
        return IMMEDIATE;
    }

    /**
     * Returns the response-level action selected by the policy.
     */
    public BackoffAction action() {
        return action;
    }

    /**
     * Returns the delay to apply before final response send.
     */
    public long delayMs() {
        return delayMs;
    }

    /**
     * Returns the internal reason bit flags used for response-level aggregation.
     */
    public int reasonMask() {
        return reasonMask;
    }

    /**
     * Returns true when the response should be scheduled through the delayed response scheduler.
     */
    public boolean delayed() {
        return action == BackoffAction.DELAYED;
    }
}
