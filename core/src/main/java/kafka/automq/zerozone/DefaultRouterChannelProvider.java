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

package kafka.automq.zerozone;

import org.apache.kafka.controller.stream.RouterChannelEpoch;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.GlobalNetworkBandwidthLimiters;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.buffer.Unpooled;

public class DefaultRouterChannelProvider implements RouterChannelProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRouterChannelProvider.class);
    public static final String WAL_TYPE = "rc";
    private final int nodeId;
    private final long nodeEpoch;
    private final short channelId;
    private final BucketURI bucketURI;
    private volatile RouterChannel routerChannel;
    private ObjectStorage objectStorage;
    private final Map<Integer, RouterChannel> routerChannels = new ConcurrentHashMap<>();
    private final String clusterId;

    private final List<EpochListener> epochListeners = new CopyOnWriteArrayList<>();
    private volatile RouterChannelEpoch epoch = new RouterChannelEpoch(-3L, -2L, 0, 0);

    public DefaultRouterChannelProvider(int nodeId, long nodeEpoch, BucketURI bucketURI, String clusterId) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.bucketURI = bucketURI;
        this.channelId = bucketURI.bucketId();
        this.clusterId = clusterId;
    }

    @Override
    public RouterChannel channel() {
        if (routerChannel != null) {
            return routerChannel;
        }
        synchronized (this) {
            if (routerChannel == null) {
                ObjectWALConfig config = ObjectWALConfig.builder()
                    .withClusterId(clusterId)
                    .withNodeId(nodeId)
                    .withEpoch(nodeEpoch)
                    .withOpenMode(OpenMode.READ_WRITE)
                    .withType(WAL_TYPE)
                    .build();
                ObjectWALService wal = new ObjectWALService(Time.SYSTEM, objectStorage(), config);
                try {
                    wal.start();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                RouterChannel routerChannel = new ObjectRouterChannel(this.nodeId, channelId, wal);
                routerChannel.nextEpoch(epoch.getCurrent());
                routerChannel.trim(epoch.getCommitted());
                this.routerChannel = routerChannel;
            }
            return routerChannel;
        }
    }

    @Override
    public RouterChannel readOnlyChannel(int node) {
        if (nodeId == node) {
            return channel();
        }
        return routerChannels.computeIfAbsent(node, nodeId -> {
            ObjectWALConfig config = ObjectWALConfig.builder().withClusterId(clusterId).withNodeId(node).withOpenMode(OpenMode.READ_ONLY).withType(WAL_TYPE).build();
            ObjectWALService wal = new ObjectWALService(Time.SYSTEM, objectStorage(), config);
            try {
                wal.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ObjectRouterChannel(nodeId, channelId, wal);
        });
    }

    @Override
    public RouterChannelEpoch epoch() {
        return epoch;
    }

    @Override
    public void addEpochListener(EpochListener listener) {
        epochListeners.add(listener);
    }

    @Override
    public void close() {
        FutureUtil.suppress(() -> routerChannel.close(), LOGGER);
        routerChannels.forEach((nodeId, channel) -> FutureUtil.suppress(channel::close, LOGGER));
    }

    @Override
    public void onChange(MetadataDelta delta, MetadataImage image) {
        if (delta.kvDelta() == null) {
            return;
        }
        ByteBuffer value = delta.kvDelta().changedKV().get(RouterChannelEpoch.ROUTER_CHANNEL_EPOCH_KEY);
        if (value == null) {
            return;
        }
        synchronized (this) {
            this.epoch = RouterChannelEpoch.decode(Unpooled.wrappedBuffer(value.slice()));
            RouterChannel routerChannel = this.routerChannel;
            if (routerChannel != null) {
                routerChannel.nextEpoch(epoch.getCurrent());
                routerChannel.trim(epoch.getCommitted());
            }
        }
        notifyEpochListeners(epoch);

    }

    private void notifyEpochListeners(RouterChannelEpoch epoch) {
        for (EpochListener listener : epochListeners) {
            try {
                listener.onNewEpoch(epoch);
            } catch (Throwable t) {
                LOGGER.error("Failed to notify epoch listener {}", listener, t);
            }
        }
    }

    synchronized ObjectStorage objectStorage() {
        if (objectStorage == null) {
            this.objectStorage = ObjectStorageFactory.instance().builder(bucketURI)
                .readWriteIsolate(true)
                .inboundLimiter(GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND))
                .outboundLimiter(GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.OUTBOUND))
                .build();
        }
        return objectStorage;
    }
}
