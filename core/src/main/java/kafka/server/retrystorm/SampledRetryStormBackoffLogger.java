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

import kafka.automq.retrystorm.RetryStormBackoffStateStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Logs periodic snapshots of retry storm dimensions that remain in delaying mode.
 */
public class SampledRetryStormBackoffLogger implements RetryStormBackoffLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampledRetryStormBackoffLogger.class);

    /**
     * Logs at most one line per delayed state snapshot.
     */
    @Override
    public void logDelayedStates(List<RetryStormBackoffStateStore.DelayedStateSnapshot> snapshots) {
        for (RetryStormBackoffStateStore.DelayedStateSnapshot snapshot : snapshots) {
            RetryStormBackoffStateStore.BackoffKey key = snapshot.key();
            LOGGER.info("Retry storm delaying state apiKey={} resource={} clientScope={} reason={} delayingSinceMs={} lastFailureMs={}",
                key.apiKey(), key.resourceKey(), key.clientScope(),
                RetryStormBackoffStateStore.reasonString(snapshot.reasonMask()),
                snapshot.delayingSinceMs(), snapshot.lastFailureMs());
        }
    }
}
