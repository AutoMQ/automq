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

package kafka.log.stream.s3;

import com.automq.stream.api.Client;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.DefaultS3BlockCache;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.compact.CompactionManager;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import kafka.log.stream.s3.failover.FailoverListener;
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;
import com.automq.stream.utils.LogContext;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DefaultS3Client implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    private final Config config;
    private final StreamMetadataManager metadataManager;

    private final ControllerRequestSender requestSender;

    private final WriteAheadLog writeAheadLog;
    private final Storage storage;

    private final S3BlockCache blockCache;

    private final ObjectManager objectManager;

    private final StreamManager streamManager;

    private final CompactionManager compactionManager;

    private final S3StreamClient streamClient;

    private final KVClient kvClient;

    private FailoverListener failover;

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
        String accessKey = this.config.accessKey();
        String secretKey = this.config.secretKey();
        boolean forcePathStyle = this.config.forcePathStyle();
        S3Operator s3Operator = DefaultS3Operator.builder().endpoint(endpoint).region(region).bucket(bucket).accessKey(accessKey).secretKey(secretKey)
                .inboundLimiter(networkInboundLimiter).outboundLimiter(networkOutboundLimiter).readWriteIsolate(true).forcePathStyle(forcePathStyle).build();
        S3Operator compactionS3Operator = DefaultS3Operator.builder().endpoint(endpoint).region(region).bucket(bucket).accessKey(accessKey).secretKey(secretKey)
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

        if (config.failoverEnable()) {
            this.failover = new FailoverListener((ControllerStreamManager) streamManager, (ControllerObjectManager) objectManager, (S3Storage) storage, brokerServer);
        }
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
}
