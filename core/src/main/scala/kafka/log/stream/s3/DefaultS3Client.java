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
import com.automq.stream.s3.exceptions.AutoMQException;
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
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.PingS3Helper;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.server.BrokerServer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKeyCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultS3Client implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    protected final Config config;
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
    private final LocalStreamRangeIndexCache localIndexCache;

    private final AutoMQVersion remoteAutoMQVersion;

    public DefaultS3Client(BrokerServer brokerServer, Config config) {
        this.brokerServer = brokerServer;
        this.config = config;
        BucketURI dataBucket = config.dataBuckets().get(0);
        long refillToken = (long) (config.networkBaselineBandwidth() * ((double) config.refillPeriodMs() / 1000));
        if (refillToken <= 0) {
            throw new IllegalArgumentException(String.format("refillToken must be greater than 0, bandwidth: %d, refill period: %dms",
                config.networkBaselineBandwidth(), config.refillPeriodMs()));
        }
        remoteAutoMQVersion = getRemoteAutoMQVersion();
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
    }

    protected WriteAheadLog buildWAL() {
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
                    .withEpoch(config.nodeEpoch())
                    .withBucketId(bucketURI.bucketId());

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
        return new ControllerStreamManager(this.metadataManager, this.requestSender, nodeId, nodeEpoch,
            this::getAutoMQVersion, failoverMode);
    }

    ObjectManager newObjectManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerObjectManager(this.requestSender, this.metadataManager, nodeId, nodeEpoch,
            this::getAutoMQVersion, failoverMode);
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

    private AutoMQVersion getAutoMQVersion() {
        if (brokerServer.metadataCache().currentImage() == MetadataImage.EMPTY) {
            // The s3stream initialized before the metadata load, so we use remoteAutoMQVersion.
            return remoteAutoMQVersion;
        } else {
            return brokerServer.metadataCache().autoMQVersion();
        }
    }

    private AutoMQVersion getRemoteAutoMQVersion() {
        CompletableFuture<AutoMQVersion> cf = new CompletableFuture<>();
        brokerServer.clientToControllerChannelManager().sendRequest(new ApiVersionsRequest.Builder(), new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                cf.completeExceptionally(new TimeoutException("Get AutoMQVersion timeout"));
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (!response.hasResponse()) {
                    cf.completeExceptionally(new AutoMQException("Get AutoMQVersion with empty response: " + response));
                    return;
                }
                ApiVersionsResponse resp = (ApiVersionsResponse) response.responseBody();
                if (resp.data().errorCode() != Errors.NONE.code()) {
                    cf.completeExceptionally(new AutoMQException(String.format("Get AutoMQVersion with error[%s] response: %s", resp.data().errorCode(), response)));
                    return;
                }
                FinalizedFeatureKeyCollection features = resp.data().finalizedFeatures();
                FinalizedFeatureKey featureKey = features.find(AutoMQVersion.FEATURE_NAME);
                cf.complete(AutoMQVersion.from(featureKey.maxVersionLevel()));
            }
        });
        try {
            return cf.get();
        } catch (Throwable e) {
            throw new AutoMQException(e);
        }
    }
}
