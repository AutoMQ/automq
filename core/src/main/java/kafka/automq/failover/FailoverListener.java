/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.automq.failover;

import kafka.automq.utils.JsonUtils;

import org.apache.kafka.image.KVDelta;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;

import com.automq.stream.api.Client;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FailoverListener implements MetadataPublisher, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverListener.class);
    private final int nodeId;
    private final Map<FailedNode, FailoverContext> recovering = new ConcurrentHashMap<>();

    private final Client client;

    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("failover-listener-%d", true), LOGGER);

    public FailoverListener(int nodeId, Client client) {
        this.nodeId = nodeId;
        this.client = client;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage image, LoaderManifest manifest) {
        try {
            getContexts(delta).ifPresent(this::onContextsChange);
        } catch (Throwable e) {
            LOGGER.error("failover listener fail", e);
        }
    }

    /**
     * Get the failover contexts from the given metadata delta.
     * It returns empty if there is no update for the failover key.
     */
    private Optional<FailoverContext[]> getContexts(MetadataDelta delta) {
        return Optional.ofNullable(delta.kvDelta())
            .map(KVDelta::changedKV)
            .map(kv -> kv.get(FailoverConstants.FAILOVER_KEY))
            .map(this::decodeContexts);
    }
    
        private FailoverContext[] decodeContexts(ByteBuffer byteBuffer) {
        ByteBuffer slice = byteBuffer.slice();
        byte[] data = new byte[slice.remaining()];
        slice.get(data);
        return JsonUtils.decode(new String(data, StandardCharsets.UTF_8), FailoverContext[].class);
    }


    private void onContextsChange(FailoverContext[] contexts) {
        Set<FailedNode> oldFailedNodes = recovering.keySet();
        Set<FailedNode> newFailedNodes = Arrays.stream(contexts)
            .filter(ctx -> ctx.getTarget() == nodeId)
            .map(FailoverContext::getFailedNode)
            .collect(Collectors.toSet());

        Set<FailedNode> completedNodes = Sets.difference(oldFailedNodes, newFailedNodes);
        completedNodes.forEach(recovering::remove);

        Set<FailedNode> needFailoverNodes = Sets.difference(newFailedNodes, oldFailedNodes);
        for (FailoverContext context : contexts) {
            FailedNode failedNode = context.getFailedNode();
            if (needFailoverNodes.contains(failedNode)) {
                recovering.put(failedNode, context);
                failover(context);
            }
        }
    }

    private void failover(FailoverContext context) {
        scheduler.execute(() -> failover0(context));
    }

    private void failover0(FailoverContext context) {
        failover0(context, 0);
    }


    private void failover0(FailoverContext context, int retryCount) {
        try {
            if (!recovering.containsKey(context.getFailedNode())) {
                return;
            }
            LOGGER.info("[FAILOVER] start with context={}, retryCount={}", context, retryCount);
            FailoverRequest request = new FailoverRequest();
            request.setNodeId(context.getNodeId());
            request.setNodeEpoch(context.getNodeEpoch());
            request.setKraftWalConfigs(context.getKraftWalConfigs());
            client.failover(request).get();
            LOGGER.info("[FAILOVER] complete with context={}, retryCount={}", context, retryCount);
        } catch (Throwable e) {
            int retryDelay = Math.min(1 << retryCount, 60);
            LOGGER.warn("[FAILOVER] fail, retry later. context={}, retryCount={}, retryDelay={}s", context, retryCount, retryDelay, e);
            if (recovering.containsKey(context.getFailedNode())) {
                scheduler.schedule(() -> failover0(context, retryCount + 1), retryDelay, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public String name() {
        return "failover-listener";
    }

    @Override
    public void close() throws Exception {
    }
}
