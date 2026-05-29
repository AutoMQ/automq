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

import kafka.server.ResourceErrorExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;

/**
 * Wraps final response sending with retry storm policy evaluation.
 *
 * <p>The gate is the integration boundary between Kafka request handlers and retry storm internals:
 * handlers keep building normal responses and quota state, while the gate either sends immediately
 * or delegates the final send callback to the scheduler.</p>
 */
public class RetryStormResponseGate {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryStormResponseGate.class);

    private final RetryStormBackoffPolicy policy;
    private final RetryStormDelayedResponseScheduler scheduler;

    /**
     * Creates a response gate over policy and scheduler.
     */
    public RetryStormResponseGate(RetryStormBackoffPolicy policy,
                                  RetryStormDelayedResponseScheduler scheduler) {
        this.policy = policy;
        this.scheduler = scheduler;
    }

    /**
     * Sends or delays a response using a shared resource error view extracted by RequestChannel.
     */
    public void sendOrDelay(RetryStormRequestContext context,
                            ResourceErrorExtractor.ResourceErrorView errorView,
                            OptionalLong delayCapMs,
                            Runnable sendNow) {
        if (!errorView.allErrorCandidate() || !policy.supports(context.apiKey())) {
            sendNow.run();
            return;
        }

        BackoffDecision decision;
        try {
            decision = policy.evaluate(
                context.apiKey(),
                errorView.errors(),
                new BackoffContext(context.clientScope(), context.nowMs()),
                delayCapMs
            );
        } catch (RuntimeException e) {
            LOGGER.warn("Retry storm response gate failed for apiKey={}, clientScope={}, send response immediately",
                context.apiKey(), context.clientScope(), e);
            decision = BackoffDecision.immediate();
        }

        if (decision.action() == BackoffAction.IMMEDIATE) {
            sendNow.run();
        } else {
            scheduler.schedule(decision.delayMs(), sendNow);
        }
    }
}
