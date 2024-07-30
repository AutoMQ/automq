/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3;

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
import com.automq.stream.s3.index.LocalStreamRangeIndexCache;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.impl.block.BlockWALService;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.IdURI;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.PingS3Helper;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.server.BrokerServer;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultS3Client implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    protected final Config config;
    private StreamMetadataManager metadataManager;

    protected ControllerRequestSender requestSender;

    protected WriteAheadLog writeAheadLog;
    protected S3Storage storage;

    protected ObjectReaderFactory objectReaderFactory;
    protected S3BlockCache blockCache;

    protected ObjectManager objectManager;

    protected StreamManager streamManager;

    protected CompactionManager compactionManager;

    protected S3StreamClient streamClient;

    protected KVClient kvClient;

    protected Failover failover;

    protected AsyncNetworkBandwidthLimiter networkInboundLimiter;
    protected AsyncNetworkBandwidthLimiter networkOutboundLimiter;

    protected BrokerServer brokerServer;
    protected LocalStreamRangeIndexCache localIndexCache;

    public DefaultS3Client(BrokerServer brokerServer, Config config) {
        this.brokerServer = brokerServer;
        this.config = config;
    }

    @Override
    public void start() {
        BucketURI dataBucket = config.dataBuckets().get(0);
        long refillToken = (long) (config.networkBaselineBandwidth() * ((double) config.refillPeriodMs() / 1000));
        if (refillToken <= 0) {
            throw new IllegalArgumentException(String.format("refillToken must be greater than 0, bandwidth: %d, refill period: %dms",
                config.networkBaselineBandwidth(), config.refillPeriodMs()));
        }
        networkInboundLimiter = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND,
            refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth());
        networkOutboundLimiter = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.OUTBOUND,
            refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth());
        // check s3 availability
        PingS3Helper pingS3Helper = PingS3Helper.builder()
            .bucket(dataBucket)
            .tagging(config.objectTagging())
            .needPrintToConsole(false)
            .build();
        pingS3Helper.pingS3();
        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(dataBucket).tagging(config.objectTagging())
            .inboundLimiter(networkInboundLimiter).outboundLimiter(networkOutboundLimiter).readWriteIsolate(true).build();
        ObjectStorage compactionobjectStorage = ObjectStorageFactory.instance().builder(dataBucket).tagging(config.objectTagging())
            .inboundLimiter(networkInboundLimiter).outboundLimiter(networkOutboundLimiter).build();
        ControllerRequestSender.RetryPolicyContext retryPolicyContext = new ControllerRequestSender.RetryPolicyContext(config.controllerRequestRetryMaxCount(),
            config.controllerRequestRetryBaseDelayMs());
        localIndexCache = new LocalStreamRangeIndexCache();
        localIndexCache.start();
        localIndexCache.init(config.nodeId(), objectStorage);
        this.objectReaderFactory = new DefaultObjectReaderFactory(objectStorage);
        this.metadataManager = new StreamMetadataManager(brokerServer, config.nodeId(), objectReaderFactory, localIndexCache);
        this.requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
        this.streamManager = newStreamManager(config.nodeId(), config.nodeEpoch(), false);
        this.objectManager = newObjectManager(config.nodeId(), config.nodeEpoch(), false);
        this.objectManager.setCommitStreamSetObjectHook(localIndexCache::updateIndexFromRequest);
        this.blockCache = new StreamReaders(this.config.blockCacheSize(), objectManager, objectStorage, objectReaderFactory);
        this.compactionManager = new CompactionManager(this.config, this.objectManager, this.streamManager, compactionobjectStorage);
        this.writeAheadLog = buildWAL();
        this.storage = new S3Storage(this.config, writeAheadLog, streamManager, objectManager, blockCache, objectStorage);
        // stream object compactions share the same object storage with stream set object compactions
        this.streamClient = new S3StreamClient(this.streamManager, this.storage, this.objectManager, compactionobjectStorage, this.config, networkInboundLimiter, networkOutboundLimiter);
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
        IdURI uri = IdURI.parse(config.walConfig());
        switch (uri.protocol()) {
            case "file":
                return BlockWALService.builder(uri).config(config).build();
            case "s3":
                ObjectStorage walObjectStorage = ObjectStorageFactory.instance().builder(BucketURI.parse(config.walConfig()))
                    .tagging(config.objectTagging())
                    .build();

                ObjectWALConfig.Builder configBuilder = ObjectWALConfig.builder().withURI(uri)
                    .withClusterId(brokerServer.clusterId())
                    .withNodeId(config.nodeId())
                    .withEpoch(config.nodeEpoch());

                return new ObjectWALService(Time.SYSTEM, walObjectStorage, configBuilder.build());
            default:
                throw new IllegalArgumentException("Invalid WAL schema: " + uri.protocol());
        }
    }

    protected StreamManager newStreamManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerStreamManager(this.metadataManager, this.requestSender, nodeId, nodeEpoch,
            this::getAutoMQVersion, failoverMode);
    }

    protected ObjectManager newObjectManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerObjectManager(this.requestSender, this.metadataManager, nodeId, nodeEpoch,
            this::getAutoMQVersion, failoverMode);
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
                return BlockWALService.recoveryBuilder(request.getDevice()).build();
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
}
