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

import com.automq.s3shell.sdk.auth.CredentialsProviderHolder;
import com.automq.s3shell.sdk.auth.EnvVariableCredentialsProvider;
import com.automq.stream.api.Client;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.cache.DefaultS3BlockCache;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.compact.CompactionManager;
import com.automq.stream.s3.failover.Failover;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.s3.failover.FailoverResponse;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;
import kafka.log.stream.s3.failover.DefaultFailoverFactory;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DefaultS3Client implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    private final Config config;
    private final StreamMetadataManager metadataManager;

    private final ControllerRequestSender requestSender;

    private final WriteAheadLog writeAheadLog;
    private final S3Storage storage;

    private final S3BlockCache blockCache;

    private final ControllerObjectManager objectManager;

    private final ControllerStreamManager streamManager;

    private final CompactionManager compactionManager;

    private final S3StreamClient streamClient;

    private final KVClient kvClient;

    private final Failover failover;

    private final AsyncNetworkBandwidthLimiter networkInboundLimiter;
    private final AsyncNetworkBandwidthLimiter networkOutboundLimiter;

    public DefaultS3Client(BrokerServer brokerServer, KafkaConfig kafkaConfig) {
        this.config = ConfigUtils.to(kafkaConfig);
        this.metadataManager = new StreamMetadataManager(brokerServer, kafkaConfig);
        String endpoint = kafkaConfig.s3Endpoint();
        String region = kafkaConfig.s3Region();
        String bucket = kafkaConfig.s3Bucket();
        networkInboundLimiter = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND,
                config.networkBaselineBandwidth(), config.refillPeriodMs(), config.networkBaselineBandwidth());
        networkOutboundLimiter = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.OUTBOUND,
                config.networkBaselineBandwidth(), config.refillPeriodMs(), config.networkBaselineBandwidth());
        List<AwsCredentialsProvider> credentialsProviders = List.of(CredentialsProviderHolder.getAwsCredentialsProvider(), EnvVariableCredentialsProvider.get());
        boolean forcePathStyle = this.config.forcePathStyle();
        S3Operator s3Operator = DefaultS3Operator.builder().endpoint(endpoint).region(region).bucket(bucket).credentialsProviders(credentialsProviders)
                .inboundLimiter(networkInboundLimiter).outboundLimiter(networkOutboundLimiter).readWriteIsolate(true).forcePathStyle(forcePathStyle).build();
        S3Operator compactionS3Operator = DefaultS3Operator.builder().endpoint(endpoint).region(region).bucket(bucket).credentialsProviders(credentialsProviders)
                .inboundLimiter(networkInboundLimiter).outboundLimiter(networkOutboundLimiter).forcePathStyle(forcePathStyle).build();
        ControllerRequestSender.RetryPolicyContext retryPolicyContext = new ControllerRequestSender.RetryPolicyContext(kafkaConfig.s3ControllerRequestRetryMaxCount(),
                kafkaConfig.s3ControllerRequestRetryBaseDelayMs());
        this.requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
        this.streamManager = new ControllerStreamManager(this.metadataManager, this.requestSender, kafkaConfig);
        this.objectManager = new ControllerObjectManager(this.requestSender, this.metadataManager, kafkaConfig);
        this.blockCache = new DefaultS3BlockCache(this.config, objectManager, s3Operator);
        this.compactionManager = new CompactionManager(this.config, this.objectManager, this.streamManager, compactionS3Operator);
        this.writeAheadLog = BlockWALService.builder(this.config.walPath(), this.config.walCapacity()).config(this.config).build();
        this.storage = new S3Storage(this.config, writeAheadLog, streamManager, objectManager, blockCache, s3Operator);
        // stream object compactions share the same s3Operator with stream set object compactions
        this.streamClient = new S3StreamClient(this.streamManager, this.storage, this.objectManager, compactionS3Operator, this.config, networkInboundLimiter, networkOutboundLimiter);
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

    Failover failover() {
        return new Failover(new DefaultFailoverFactory(streamManager, objectManager), (wal, sm, om, logger) -> {
            try {
                storage.recover(wal, sm, om, logger);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }
}
