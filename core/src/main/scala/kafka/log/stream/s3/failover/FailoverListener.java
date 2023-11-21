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

package kafka.log.stream.s3.failover;

import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.failover.Failover;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import kafka.log.stream.s3.metadata.MetadataListener;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.server.BrokerServer;
import org.apache.kafka.common.metadata.FailoverContextRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.stream.FailoverStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailoverListener implements MetadataListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverListener.class);
    private final int nodeId;
    private final Failover failover;
    private final Map<Integer, FailoverContextRecord> recovering = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("failover-listener-%d", true), LOGGER);

    public FailoverListener(ControllerStreamManager streamManager, ControllerObjectManager objectManager, S3Storage s3Storage, BrokerServer brokerServer) {
        brokerServer.metadataListener().registerMetadataListener(this);
        this.nodeId = brokerServer.config().nodeId();
        this.failover = new Failover(new DefaultFailoverFactory(streamManager, objectManager), (wal, sm, om, logger) -> {
            try {
                s3Storage.recover(wal, sm, om, logger);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void onChange(MetadataDelta delta, MetadataImage image) {
        try {
            onChange0(delta, image);
        } catch (Throwable e) {
            LOGGER.error("failover listener fail, {}", image.failoverContext().contexts(), e);
        }
    }

    public void onChange0(MetadataDelta delta, MetadataImage image) {
        Map<Integer, FailoverContextRecord> contexts = image.failoverContext().contexts();
        for (FailoverContextRecord context : contexts.values()) {
            if (context.targetNodeId() != nodeId) {
                recovering.remove(context.failedNodeId());
                continue;
            }
            if (FailoverStatus.DONE.name().equals(context.status())) {
                recovering.remove(context.failedNodeId());
            } else if (FailoverStatus.RECOVERING.name().equals(context.status()) && !recovering.containsKey(context.failedNodeId())) {
                recovering.put(context.failedNodeId(), context);
                failover(context.failedNodeId());
            }
        }
    }

    private void failover(int nodeId) {
        FailoverContextRecord context = recovering.get(nodeId);
        if (context == null) {
            return;
        }
        FailoverRequest request = new FailoverRequest();
        request.setNodeId(context.failedNodeId());
        request.setVolumeId(context.volumeId());
        request.setDevice(context.device());
        failover.failover(request).whenCompleteAsync((nil, ex) -> {
            if (ex != null) {
                if (recovering.containsKey(request.getNodeId())) {
                    scheduler.schedule(() -> failover(request.getNodeId()), 10, TimeUnit.SECONDS);
                }
            }
        }, scheduler);
    }
}
