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
package kafka.server;

import org.apache.kafka.clients.admin.ClusterEventPublisher;
import org.apache.kafka.clients.admin.RequestErrorEventData;
import org.apache.kafka.common.protocol.ApiKeys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class RequestErrorAccumulator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RequestErrorAccumulator.class);

    private static final int MAX_CLIENTS = 10;

    record ErrorKey(short apiKey, short errorCode, String resource) { }

    static class ErrorBucket {
        final LongAdder count = new LongAdder();
        final Set<String> clientIps = ConcurrentHashMap.newKeySet();
        final Set<String> clientIds = ConcurrentHashMap.newKeySet();
    }

    private final ConcurrentHashMap<ErrorKey, ErrorBucket> buckets = new ConcurrentHashMap<>();
    private final int brokerId;
    private final long flushIntervalMs;
    private final ScheduledExecutorService scheduler;

    public RequestErrorAccumulator(int brokerId, long flushIntervalMs) {
        this.brokerId = brokerId;
        this.flushIntervalMs = flushIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "request-error-accumulator-flush");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(this::flush,
            flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    public static boolean isRecordable(short errorCode) {
        return errorCode != 0;
    }

    public void record(short apiKey, short errorCode, String resource,
                       String clientIp, String clientId) {
        ErrorKey key = new ErrorKey(apiKey, errorCode, resource);
        ErrorBucket bucket = buckets.computeIfAbsent(key, k -> new ErrorBucket());
        bucket.count.increment();
        if (bucket.clientIps.size() < MAX_CLIENTS) {
            bucket.clientIps.add(clientIp);
        }
        if (bucket.clientIds.size() < MAX_CLIENTS) {
            bucket.clientIds.add(clientId);
        }
    }

    void flush() {
        try {
            doFlush();
        } catch (Throwable e) {
            log.warn("Error flushing request error events", e);
        }
    }

    private void doFlush() {
        double intervalSec = flushIntervalMs / 1000.0;
        Iterator<Map.Entry<ErrorKey, ErrorBucket>> it = buckets.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ErrorKey, ErrorBucket> entry = it.next();
            // Remove the entry atomically before reading its values so that any new records
            // arriving after this point go into a fresh bucket and are not silently dropped.
            it.remove();
            ErrorKey key = entry.getKey();
            ErrorBucket bucket = entry.getValue();
            long count = bucket.count.sum();
            if (count == 0) {
                continue;
            }

            String apiName = ApiKeys.forId(key.apiKey()).name;

            RequestErrorEventData data = new RequestErrorEventData()
                .setApiKey(key.apiKey())
                .setErrorCode(key.errorCode())
                .setClientIps(List.copyOf(bucket.clientIps))
                .setClientIds(List.copyOf(bucket.clientIds))
                .setRps(count / intervalSec);
            if (!key.resource().isEmpty()) {
                data.setResource(key.resource());
            }

            String subject = key.resource().isEmpty()
                ? apiName : apiName + ":" + key.resource();

            ClusterEventPublisher.publish(
                RequestErrorEventData.TYPE,
                "/automq/broker/" + brokerId,
                subject,
                RequestErrorEventData.DATA_SCHEMA,
                data.toByteArray());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
