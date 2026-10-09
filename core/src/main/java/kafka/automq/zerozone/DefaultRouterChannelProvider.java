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

import org.apache.kafka.controller.stream.KVKey;
import org.apache.kafka.controller.stream.RouterChannelEpoch;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

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
    private final List<BucketURI> buckets;
    private volatile RouterChannel routerChannel;
    private final Map<Short, ObjectStorage> objectStorages = new ConcurrentHashMap<>();
    private final Map<Integer, RouterChannel> routerChannels = new ConcurrentHashMap<>();
    private final String clusterId;

    private final List<EpochListener> epochListeners = new CopyOnWriteArrayList<>();
    private volatile RouterChannelEpoch epoch = new RouterChannelEpoch(-3L, -2L, 0, 0);

    /**
     * Creates a provider whose local writer and remote readers share per-bucket storage clients.
     * Bucket IDs must remain stable while offsets referencing them are retained.
     */
    public DefaultRouterChannelProvider(int nodeId, long nodeEpoch, List<BucketURI> buckets, String clusterId) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.buckets = List.copyOf(buckets);
        this.clusterId = clusterId;
    }

    @Override
    public RouterChannel channel() {
        if (routerChannel != null) {
            return routerChannel;
        }
        synchronized (this) {
            if (routerChannel == null) {
                RouterChannel routerChannel = newChannel(nodeId, false);
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
        return routerChannels.computeIfAbsent(node, id -> newChannel(id, true));
    }

    private RouterChannel newChannel(int ownerNodeId, boolean readOnly) {
        return new MultiBucketsRouterChannel(ownerNodeId, buckets, readOnly, (bucket, bucketReadOnly) -> {
            ObjectWALConfig.Builder builder = ObjectWALConfig.builder()
                .withClusterId(clusterId)
                .withNodeId(ownerNodeId)
                .withBucketId(bucket.bucketId())
                .withOpenMode(bucketReadOnly ? OpenMode.READ_ONLY : OpenMode.READ_WRITE)
                .withType(WAL_TYPE)
                .withManualMode(true);
            if (!bucketReadOnly) {
                builder.withURI(bucket.toIdURI()).withEpoch(nodeEpoch);
            }
            ObjectWALConfig config = builder.build();
            ObjectStorage storage = objectStorage(bucket);
            ObjectWALService wal = new ObjectWALService(Time.SYSTEM, storage, config);
            try {
                wal.start();
                return wal;
            } catch (Throwable error) {
                wal.shutdownGracefully();
                throw new RuntimeException("Failed to start router channel WAL", error);
            }
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
        if (routerChannel != null) {
            FutureUtil.suppress(() -> routerChannel.close().get(), LOGGER);
        }
        routerChannels.forEach((nodeId, channel) -> FutureUtil.suppress(() -> channel.close().get(), LOGGER));
        objectStorages.values().forEach(storage -> FutureUtil.suppress(storage::close, LOGGER));
    }

    @Override
    public void onChange(MetadataDelta delta, MetadataImage image) {
        if (delta.kvDelta() == null) {
            return;
        }
        ByteBuffer value = delta.kvDelta().getChangedKV(KVKey.of(RouterChannelEpoch.ROUTER_CHANNEL_EPOCH_KEY));
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

    synchronized ObjectStorage objectStorage(BucketURI bucket) {
        return objectStorages.computeIfAbsent(bucket.bucketId(), id -> ObjectStorageFactory.instance().builder(bucket)
            .readWriteIsolate(true)
            .inboundLimiter(GlobalNetworkBandwidthLimiters.instance().inbound())
            .outboundLimiter(GlobalNetworkBandwidthLimiters.instance().outbound())
            .build());
    }
}
