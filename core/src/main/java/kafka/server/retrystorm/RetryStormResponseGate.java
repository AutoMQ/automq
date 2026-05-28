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

import java.util.Map;

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
    private final Map<ApiKeys, RetryStormResponseSummaryExtractor> extractorRegistry;

    /**
     * Creates a response gate over policy, scheduler, logger, and API-specific summary extractors.
     */
    public RetryStormResponseGate(RetryStormBackoffPolicy policy,
                                  RetryStormDelayedResponseScheduler scheduler,
                                  RetryStormBackoffLogger logger,
                                  Map<ApiKeys, RetryStormResponseSummaryExtractor> extractorRegistry) {
        this.policy = policy;
        this.scheduler = scheduler;
        this.logger = logger;
        this.extractorRegistry = extractorRegistry;
    }

    /**
     * Sends a response immediately or schedules the supplied final-send callback after policy delay.
     */
    public void sendOrDelay(RetryStormRequestContext context, Object request, Object response, Runnable sendNow) {
        RetryStormResponseSummaryExtractor extractor = extractorRegistry.get(context.apiKey());
        if (extractor == null) {
            sendNow.run();
            return;
        }

        ResponseSummary summary = extractor.apply(request, response);
        BackoffDecision decision = policy.evaluate(
            context.apiKey(),
            new RequestSummary(),
            summary,
            new BackoffContext(context.clientScope(), context.nowMs())
        );
        if (decision.action() == BackoffAction.IMMEDIATE) {
            sendNow.run();
        } else {
            logger.logDelayed(context, summary, decision);
            scheduler.schedule(request, response, decision.delayMs(), decision.reason(), sendNow);
        }
    }
}
