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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultS3Client implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    private final Config config;
    private final StreamMetadataManager metadataManager;

    private final ControllerRequestSender requestSender;

    private final WriteAheadLog writeAheadLog;
    private final S3Storage storage;

    private final ObjectReaderFactory objectReaderFactory;
    private final S3BlockCache blockCache;

    private final ObjectManager objectManager;

    private final StreamManager streamManager;

    private final CompactionManager compactionManager;

    private final S3StreamClient streamClient;

    private final KVClient kvClient;

    private final Failover failover;

    private final AsyncNetworkBandwidthLimiter networkInboundLimiter;
    private final AsyncNetworkBandwidthLimiter networkOutboundLimiter;

    private final BrokerServer brokerServer;

    public DefaultS3Client(BrokerServer brokerServer, Config config) {
        this.brokerServer = brokerServer;
        this.config = config;
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
        this.objectReaderFactory = new DefaultObjectReaderFactory(objectStorage);
        this.metadataManager = new StreamMetadataManager(brokerServer, config.nodeId(), objectReaderFactory);
        this.requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
        this.streamManager = newStreamManager(config.nodeId(), config.nodeEpoch(), false);
        this.objectManager = newObjectManager(config.nodeId(), config.nodeEpoch(), false);
        this.blockCache = new StreamReaders(this.config.blockCacheSize(), objectManager, objectStorage, objectReaderFactory);
        this.compactionManager = new CompactionManager(this.config, this.objectManager, this.streamManager, compactionobjectStorage);
        this.writeAheadLog = buildWAL();
        this.storage = new S3Storage(this.config, writeAheadLog, streamManager, objectManager, blockCache, objectStorage);
        // stream object compactions share the same object storage with stream set object compactions
        this.streamClient = new S3StreamClient(this.streamManager, this.storage, this.objectManager, compactionobjectStorage, this.config, networkInboundLimiter, networkOutboundLimiter);
        this.kvClient = new ControllerKVClient(this.requestSender);
        this.failover = failover();

        S3StreamThreadPoolMonitor.config(new LogContext("ThreadPoolMonitor").logger("s3.threads.logger"), TimeUnit.SECONDS.toMillis(5));
        S3StreamThreadPoolMonitor.init();
    }

    private WriteAheadLog buildWAL() {
        BucketURI bucketURI;
        try {
            bucketURI = BucketURI.parse(config.walPath());
        } catch (Exception e) {
            bucketURI = BucketURI.parse("0@file://" + config.walPath());
        }
        switch (bucketURI.protocol()) {
            case "file":
                return BlockWALService.builder(this.config.walPath(), this.config.walCapacity()).config(this.config).build();
            case "s3":
                ObjectStorage walObjectStorage = ObjectStorageFactory.instance().builder(bucketURI)
                    .tagging(config.objectTagging())
                    .build();

                ObjectWALConfig.Builder configBuilder = ObjectWALConfig.builder()
                    .withClusterId(brokerServer.clusterId())
                    .withNodeId(config.nodeId())
                    .withEpoch(config.nodeEpoch());

                String batchInterval = bucketURI.extensionString("batchInterval");
                if (StringUtils.isNumeric(batchInterval)) {
                    configBuilder.withBatchInterval(Long.parseLong(batchInterval));
                }
                String maxBytesInBatch = bucketURI.extensionString("maxBytesInBatch");
                if (StringUtils.isNumeric(maxBytesInBatch)) {
                    configBuilder.withMaxBytesInBatch(Long.parseLong(maxBytesInBatch));
                }
                String maxUnflushedBytes = bucketURI.extensionString("maxUnflushedBytes");
                if (StringUtils.isNumeric(maxUnflushedBytes)) {
                    configBuilder.withMaxUnflushedBytes(Long.parseLong(maxUnflushedBytes));
                }
                String maxInflightUploadCount = bucketURI.extensionString("maxInflightUploadCount");
                if (StringUtils.isNumeric(maxInflightUploadCount)) {
                    configBuilder.withMaxInflightUploadCount(Integer.parseInt(maxInflightUploadCount));
                }
                String readAheadObjectCount = bucketURI.extensionString("readAheadObjectCount");
                if (StringUtils.isNumeric(readAheadObjectCount)) {
                    configBuilder.withReadAheadObjectCount(Integer.parseInt(readAheadObjectCount));
                }
                return new ObjectWALService(Time.SYSTEM, walObjectStorage, configBuilder.build());
            default:
                throw new IllegalArgumentException("Invalid WAL schema: " + bucketURI.protocol());
        }
    }

    @Override
    public void start() {
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

    StreamManager newStreamManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerStreamManager(this.metadataManager, this.requestSender, nodeId, nodeEpoch, () -> brokerServer.metadataCache().autoMQVersion(), failoverMode);
    }

    ObjectManager newObjectManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerObjectManager(this.requestSender, this.metadataManager, nodeId, nodeEpoch, () -> brokerServer.metadataCache().autoMQVersion(), failoverMode);
    }

    Failover failover() {
        return new Failover(new FailoverFactory() {
            @Override
            public StreamManager getStreamManager(int nodeId, long nodeEpoch) {
                return newStreamManager(nodeId, nodeEpoch, true);
            }

            @Override
            public ObjectManager getObjectManager(int nodeId, long nodeEpoch) {
                return newObjectManager(nodeId, nodeEpoch, true);
            }
        }, (wal, sm, om, logger) -> {
            try {
                storage.recover(wal, sm, om, logger);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }
}
