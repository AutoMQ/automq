/*
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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

/**
 * This class bridges the gap for components that _should_ be compatible with
 * the public ProcessorContext interface, but have come to depend on features
 * in InternalProcessorContext. In theory, all the features adapted here could
 * migrate to the public interface, so each method in this class should reference
 * the ticket that would ultimately obviate it.
 */
public final class ProcessorContextUtils {

    private ProcessorContextUtils() {}

    /**
     * Note that KIP-622 would move currentSystemTimeMs to ProcessorContext,
     * removing the need for this method.
     */
    public static long getCurrentSystemTime(final ProcessorContext context) {
        return context instanceof InternalProcessorContext
            ? ((InternalProcessorContext) context).currentSystemTimeMs()
            : System.currentTimeMillis();
    }

    /**
     * Should be removed as part of KAFKA-10217
     */
    public static StreamsMetricsImpl getMetricsImpl(final ProcessorContext context) {
        return (StreamsMetricsImpl) context.metrics();
    }

    public static String changelogFor(final ProcessorContext context, final String storeName) {
        return context instanceof InternalProcessorContext
            ? ((InternalProcessorContext) context).changelogFor(storeName)
            : ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
    }
}
