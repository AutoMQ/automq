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

package kafka.log.stream.s3;

import kafka.autobalancer.metricsreporter.metric.Derivator;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.node.NodeManager;
import kafka.log.stream.s3.node.NodeManagerStub;
import kafka.log.stream.s3.node.NoopNodeManager;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.log.stream.s3.wal.BootstrapWalV1;
import kafka.log.stream.s3.wal.DefaultWalFactory;
import kafka.server.BrokerServer;

import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.api.Client;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.cache.blockcache.DefaultObjectReaderFactory;
import com.automq.stream.s3.cache.blockcache.ObjectReaderFactory;
import com.automq.stream.s3.cache.blockcache.StreamReaders;
import com.automq.stream.s3.compact.CompactionManager;
import com.automq.stream.s3.failover.Failover;
import com.automq.stream.s3.failover.FailoverFactory;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.s3.failover.FailoverResponse;
import com.automq.stream.s3.failover.ForceCloseStorageFailureHandler;
import com.automq.stream.s3.failover.HaltStorageFailureHandler;
import com.automq.stream.s3.failover.StorageFailureHandlerChain;
import com.automq.stream.s3.index.LocalStreamRangeIndexCache;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.GlobalNetworkBandwidthLimiters;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.DefaultWalHandle;
import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.WalHandle;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_BACKGROUND;
import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_KEY;
import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_MAIN;

public class DefaultS3Client implements Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    protected final Config config;
    protected final Derivator networkInboundRate = new Derivator();
    protected final Derivator networkOutboundRate = new Derivator();
    private StreamMetadataManager metadataManager;

    protected ControllerRequestSender requestSender;

    protected ObjectStorage mainObjectStorage;
    protected ObjectStorage backgroundObjectStorage;

    protected WriteAheadLog writeAheadLog;
    protected StorageFailureHandlerChain storageFailureHandlerChain;
    protected S3Storage storage;

    protected ObjectReaderFactory objectReaderFactory;
    protected S3BlockCache blockCache;

    protected ObjectManager objectManager;

    protected StreamManager streamManager;

    protected NodeManager nodeManager;

    protected CompactionManager compactionManager;

    protected S3StreamClient streamClient;

    protected KVClient kvClient;

    protected Failover failover;

    protected NetworkBandwidthLimiter networkInboundLimiter;
    protected NetworkBandwidthLimiter networkOutboundLimiter;

    protected BrokerServer brokerServer;
    protected LocalStreamRangeIndexCache localIndexCache;

    public DefaultS3Client(BrokerServer brokerServer, Config config) {
        this.brokerServer = brokerServer;
        this.config = config;
    }

    @Override
    public void start() {
        long refillToken = (long) (config.networkBaselineBandwidth() * ((double) config.refillPeriodMs() / 1000));
        if (refillToken <= 0) {
            throw new IllegalArgumentException(String.format("refillToken must be greater than 0, bandwidth: %d, refill period: %dms",
                config.networkBaselineBandwidth(), config.refillPeriodMs()));
        }
        GlobalNetworkBandwidthLimiters.instance().setup(AsyncNetworkBandwidthLimiter.Type.INBOUND,
            refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth());
        networkInboundLimiter = GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND);
        S3StreamMetricsManager.registerNetworkAvailableBandwidthSupplier(AsyncNetworkBandwidthLimiter.Type.INBOUND, () ->
            config.networkBaselineBandwidth() - (long) networkInboundRate.derive(
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()), NetworkStats.getInstance().networkInboundUsageTotal().get()));
        // Use a larger token pool for outbound traffic to avoid spikes caused by Upload WAL affecting tail-reading performance.
        GlobalNetworkBandwidthLimiters.instance().setup(AsyncNetworkBandwidthLimiter.Type.OUTBOUND,
            refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth() * 5);
        networkOutboundLimiter = GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.OUTBOUND);
        S3StreamMetricsManager.registerNetworkAvailableBandwidthSupplier(AsyncNetworkBandwidthLimiter.Type.OUTBOUND, () ->
            config.networkBaselineBandwidth() - (long) networkOutboundRate.derive(
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()), NetworkStats.getInstance().networkOutboundUsageTotal().get()));

        this.localIndexCache = new LocalStreamRangeIndexCache();
        this.objectReaderFactory = new DefaultObjectReaderFactory(() -> this.mainObjectStorage);
        this.metadataManager = new StreamMetadataManager(brokerServer, config.nodeId(), objectReaderFactory, localIndexCache);
        this.requestSender = new ControllerRequestSender(brokerServer, new ControllerRequestSender.RetryPolicyContext(config.controllerRequestRetryMaxCount(),
            config.controllerRequestRetryBaseDelayMs()));
        this.streamManager = newStreamManager(config.nodeId(), config.nodeEpoch(), false);
        this.objectManager = newObjectManager(config.nodeId(), config.nodeEpoch(), false);
        this.mainObjectStorage = newMainObjectStorage();
        if (!mainObjectStorage.readinessCheck()) {
            throw new IllegalArgumentException(String.format("%s is not ready", config.dataBuckets()));
        }
        this.backgroundObjectStorage = newBackgroundObjectStorage();
        localIndexCache.init(config.nodeId(), backgroundObjectStorage);
        localIndexCache.start();
        this.streamManager.setStreamCloseHook(streamId -> localIndexCache.uploadOnStreamClose());
        this.objectManager.setCommitStreamSetObjectHook(localIndexCache::updateIndexFromRequest);
        this.blockCache = new StreamReaders(this.config.blockCacheSize(), objectManager, mainObjectStorage, objectReaderFactory);
        this.compactionManager = new CompactionManager(this.config, this.objectManager, this.streamManager, backgroundObjectStorage);
        this.writeAheadLog = buildWAL();
        this.storageFailureHandlerChain = new StorageFailureHandlerChain();
        this.storage = newS3Storage();
        // stream object compactions share the same object storage with stream set object compactions
        this.streamClient = new S3StreamClient(this.streamManager, this.storage, this.objectManager, backgroundObjectStorage, this.config, networkInboundLimiter, networkOutboundLimiter);
        storageFailureHandlerChain.addHandler(new ForceCloseStorageFailureHandler(streamClient));
        storageFailureHandlerChain.addHandler(new HaltStorageFailureHandler());
        this.streamClient.registerStreamLifeCycleListener(localIndexCache);
        this.kvClient = new ControllerKVClient(this.requestSender);
        this.failover = failover();

        S3StreamThreadPoolMonitor.config(new LogContext("ThreadPoolMonitor").logger("s3.threads.logger"), TimeUnit.SECONDS.toMillis(5));
        S3StreamThreadPoolMonitor.init();

        this.storage.startup();
        this.compactionManager.start();
        LOGGER.info("S3Client started");
    }

    @Override
    public void shutdown() {
        this.compactionManager.shutdown();
        this.streamClient.shutdown();
        this.storage.shutdown();
        this.networkInboundLimiter.shutdown();
        this.networkOutboundLimiter.shutdown();
        this.requestSender.shutdown();
        LOGGER.info("S3Client shutdown successfully");
    }

    @Override
    public StreamClient streamClient() {
        return this.streamClient;
    }

    @Override
    public KVClient kvClient() {
        return this.kvClient;
    }

    @Override
    public CompletableFuture<FailoverResponse> failover(FailoverRequest request) {
        return this.failover.failover(request);
    }

    protected WriteAheadLog buildWAL() {
        String clusterId = brokerServer.clusterId();
        WalHandle walHandle = new DefaultWalHandle(clusterId);
        WalFactory factory = new DefaultWalFactory(config.nodeId(), config.objectTagging(), networkInboundLimiter, networkOutboundLimiter);
        return new BootstrapWalV1(config.nodeId(), config.nodeEpoch(), config.walConfig(), false, factory, getNodeManager(), walHandle);
    }

    protected ObjectStorage newMainObjectStorage() {
        return ObjectStorageFactory.instance().builder()
            .buckets(config.dataBuckets())
            .tagging(config.objectTagging())
            .extension(EXTENSION_TYPE_KEY, EXTENSION_TYPE_MAIN)
            .readWriteIsolate(true)
            .inboundLimiter(networkInboundLimiter)
            .outboundLimiter(networkOutboundLimiter)
            .threadPrefix("main")
            .build();
    }

    protected ObjectStorage newBackgroundObjectStorage() {
        return ObjectStorageFactory.instance().builder()
            .buckets(config.dataBuckets())
            .tagging(config.objectTagging())
            .extension(EXTENSION_TYPE_KEY, EXTENSION_TYPE_BACKGROUND)
            .readWriteIsolate(false)
            .inboundLimiter(networkInboundLimiter)
            .outboundLimiter(networkOutboundLimiter)
            .threadPrefix("background")
            .build();
    }

    protected StreamManager newStreamManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerStreamManager(this.metadataManager, this.requestSender, nodeId, nodeEpoch,
            this::getAutoMQVersion, failoverMode);
    }

    protected ObjectManager newObjectManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerObjectManager(this.requestSender, this.metadataManager, nodeId, nodeEpoch,
            this::getAutoMQVersion, failoverMode);
    }

    protected S3Storage newS3Storage() {
        return new S3Storage(config, writeAheadLog, streamManager, objectManager, blockCache, mainObjectStorage, storageFailureHandlerChain);
    }

    protected Failover failover() {
        return new Failover(new FailoverFactory() {
            @Override
            public StreamManager getStreamManager(int nodeId, long nodeEpoch) {
                return newStreamManager(nodeId, nodeEpoch, true);
            }

            @Override
            public ObjectManager getObjectManager(int nodeId, long nodeEpoch) {
                return newObjectManager(nodeId, nodeEpoch, true);
            }

            @Override
            public WriteAheadLog getWal(FailoverRequest request) {
                String clusterId = brokerServer.clusterId();
                int nodeId = request.getNodeId();
                long nodeEpoch = request.getNodeEpoch();
                WalHandle walHandle = new DefaultWalHandle(clusterId);
                WalFactory factory = new DefaultWalFactory(nodeId, config.objectTagging(), networkInboundLimiter, networkOutboundLimiter);
                NodeManager nodeManager = new NodeManagerStub(requestSender, nodeId, nodeEpoch, Collections.emptyMap());
                return new BootstrapWalV1(nodeId, nodeEpoch, request.getKraftWalConfigs(), true, factory, nodeManager, walHandle);
            }
        }, (wal, sm, om, logger) -> {
            try {
                storage.recover(wal, sm, om, logger);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected AutoMQVersion getAutoMQVersion() {
        if (brokerServer.metadataCache().currentImage() == MetadataImage.EMPTY) {
            throw new IllegalStateException("The image should be loaded first");
        }
        return brokerServer.metadataCache().autoMQVersion();
    }

    private NodeManager getNodeManager() {
        if (this.nodeManager == null) {
            this.nodeManager = config.version().isWalRegistrationSupported()
                ? new NodeManagerStub(this.requestSender, config.nodeId(), config.nodeEpoch(), new HashMap<>())
                : new NoopNodeManager(config.nodeId(), config.nodeEpoch());
        }
        return this.nodeManager;
    }
}
