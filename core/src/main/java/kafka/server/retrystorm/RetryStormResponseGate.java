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

import java.util.OptionalLong;

/**
 * Wraps final response sending with retry storm policy evaluation.
 *
 * <p>The gate is the integration boundary between Kafka request handlers and retry storm internals:
 * handlers keep building normal responses and quota state, while the gate either sends immediately
 * or delegates the final send callback to the scheduler.</p>
 */
public class RetryStormResponseGate {
    private final RetryStormBackoffPolicy policy;
    private final RetryStormDelayedResponseScheduler scheduler;
    private final RetryStormBackoffLogger logger;

    /**
     * Creates a response gate over policy, scheduler, and logger.
     */
    public RetryStormResponseGate(RetryStormBackoffPolicy policy,
                                  RetryStormDelayedResponseScheduler scheduler,
                                  RetryStormBackoffLogger logger) {
        this.policy = policy;
        this.scheduler = scheduler;
        this.logger = logger;
    }

    /**
     * Sends or delays a response using a shared resource error view extracted by RequestChannel.
     */
    public void sendOrDelay(RetryStormRequestContext context,
                            ResourceErrorExtractor.ResourceErrorView errorView,
                            OptionalLong delayCapMs,
                            Object request,
                            Object response,
                            Runnable sendNow) {
        if (!errorView.allErrorCandidate() || !policy.supports(context.apiKey())) {
            sendNow.run();
            return;
        }

        BackoffDecision decision = policy.evaluate(
            context.apiKey(),
            errorView.errors(),
            new BackoffContext(context.clientScope(), context.nowMs()),
            delayCapMs
        );
        if (decision.action() == BackoffAction.IMMEDIATE) {
            sendNow.run();
        } else {
            logger.logDelayed(context, errorView.errors(), decision);
            scheduler.schedule(request, response, decision.delayMs(), decision.reason(), sendNow);
        }
    }
}
