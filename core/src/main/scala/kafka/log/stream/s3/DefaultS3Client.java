/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3;

import com.automq.shell.auth.CredentialsProviderHolder;
import com.automq.shell.auth.EnvVariableCredentialsProvider;
import com.automq.stream.api.Client;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.cache.blockcache.StreamReaders;
import com.automq.stream.s3.compact.CompactionManager;
import com.automq.stream.s3.failover.Failover;
import com.automq.stream.s3.failover.FailoverFactory;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.s3.failover.FailoverResponse;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.PingS3Helper;
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.server.BrokerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class DefaultS3Client implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    private final Config config;
    private final StreamMetadataManager metadataManager;

    private final ControllerRequestSender requestSender;

    private final WriteAheadLog writeAheadLog;
    private final S3Storage storage;

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
        this.metadataManager = new StreamMetadataManager(brokerServer, config.nodeId());
        String endpoint = config.endpoint();
        String region = config.region();
        String bucket = config.bucket();
        long refillToken = (long) (config.networkBaselineBandwidth() * ((double) config.refillPeriodMs() / 1000));
        if (refillToken <= 0) {
            throw new IllegalArgumentException(String.format("refillToken must be greater than 0, bandwidth: %d, refill period: %dms",
                    config.networkBaselineBandwidth(), config.refillPeriodMs()));
        }
        networkInboundLimiter = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND,
                refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth());
        networkOutboundLimiter = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.OUTBOUND,
                refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth());
        List<AwsCredentialsProvider> credentialsProviders = List.of(CredentialsProviderHolder.getAwsCredentialsProvider(), EnvVariableCredentialsProvider.get());
        boolean forcePathStyle = this.config.forcePathStyle();
        // check s3 availability
        PingS3Helper pingS3Helper = PingS3Helper.builder()
                .endpoint(endpoint)
                .bucket(bucket)
                .region(region)
                .credentialsProviders(credentialsProviders)
                .isForcePathStyle(forcePathStyle)
                .tagging(config.objectTagging())
                .needPrintToConsole(false)
                .build();
        pingS3Helper.pingS3();
        ObjectStorage objectStorage = AwsObjectStorage.builder().endpoint(endpoint).region(region).bucket(bucket).credentialsProviders(credentialsProviders).tagging(config.objectTagging())
                .inboundLimiter(networkInboundLimiter).outboundLimiter(networkOutboundLimiter).readWriteIsolate(true).forcePathStyle(forcePathStyle).build();
        ObjectStorage compactionobjectStorage = AwsObjectStorage.builder().endpoint(endpoint).region(region).bucket(bucket).credentialsProviders(credentialsProviders).tagging(config.objectTagging())
                .inboundLimiter(networkInboundLimiter).outboundLimiter(networkOutboundLimiter).forcePathStyle(forcePathStyle).build();
        ControllerRequestSender.RetryPolicyContext retryPolicyContext = new ControllerRequestSender.RetryPolicyContext(config.controllerRequestRetryMaxCount(),
                config.controllerRequestRetryBaseDelayMs());
        this.requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
        this.streamManager = newStreamManager(config.nodeId(), config.nodeEpoch(), false);
        this.objectManager = newObjectManager(config.nodeId(), config.nodeEpoch(), false);
        this.blockCache = new StreamReaders(this.config.blockCacheSize(), objectManager, objectStorage);
        this.compactionManager = new CompactionManager(this.config, this.objectManager, this.streamManager, compactionobjectStorage);
        this.writeAheadLog = BlockWALService.builder(this.config.walPath(), this.config.walCapacity()).config(this.config).build();
        this.storage = new S3Storage(this.config, writeAheadLog, streamManager, objectManager, blockCache, objectStorage);
        // stream object compactions share the same s3Operator with stream set object compactions
        this.streamClient = new S3StreamClient(this.streamManager, this.storage, this.objectManager, compactionobjectStorage, this.config, networkInboundLimiter, networkOutboundLimiter);
        this.kvClient = new ControllerKVClient(this.requestSender);
        this.failover = failover();

        S3StreamThreadPoolMonitor.config(new LogContext("ThreadPoolMonitor").logger("s3.threads.logger"), TimeUnit.SECONDS.toMillis(5));
        S3StreamThreadPoolMonitor.init();
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
