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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Sampled logger that reports the first delayed decision and then every configured interval.
 */
public class SampledRetryStormBackoffLogger implements RetryStormBackoffLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampledRetryStormBackoffLogger.class);

    private final long sampleEvery;
    private final AtomicLong delayedDecisions = new AtomicLong(0L);

    /**
     * Creates a sampled logger that logs the first decision and then every 1000 decisions.
     */
    public SampledRetryStormBackoffLogger() {
        this(1000L);
    }

    /**
     * Creates a sampled logger with a caller-defined sampling interval.
     */
    public SampledRetryStormBackoffLogger(long sampleEvery) {
        this.sampleEvery = sampleEvery;
    }

    /**
     * Logs selected delayed response decisions with API key, delay, reason, and affected resources.
     */
    @Override
    public void logDelayed(RetryStormRequestContext context, ResponseSummary responseSummary, BackoffDecision decision) {
        long count = delayedDecisions.incrementAndGet();
        if (sampleEvery <= 1 || count == 1 || count % sampleEvery == 0) {
            String resources = responseSummary.resources().stream()
                .map(ResourceResult::resourceKey)
                .collect(Collectors.joining(","));
            LOGGER.info("Retry storm delayed response apiKey={} delayMs={} reason={} resources={}",
                context.apiKey(), decision.delayMs(), decision.reason(), resources);
        }
    }
}
