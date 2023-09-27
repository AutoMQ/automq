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
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultS3Client implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    private final Config config;
    private final StreamMetadataManager metadataManager;

    private final ControllerRequestSender requestSender;

    private final S3Operator operator;

    private final WriteAheadLog writeAheadLog;
    private final Storage storage;

    private final S3BlockCache blockCache;

    private final ObjectManager objectManager;

    private final StreamManager streamManager;

    private final CompactionManager compactionManager;

    private final S3StreamClient streamClient;

    private final KVClient kvClient;

    public DefaultS3Client(BrokerServer brokerServer, KafkaConfig kafkaConfig, S3Operator operator) {
        this.config = ConfigUtils.to(kafkaConfig);
        this.metadataManager = new StreamMetadataManager(brokerServer, kafkaConfig);
        this.operator = operator;
        ControllerRequestSender.RetryPolicyContext retryPolicyContext = new ControllerRequestSender.RetryPolicyContext(kafkaConfig.s3ControllerRequestRetryMaxCount(),
                kafkaConfig.s3ControllerRequestRetryBaseDelayMs());
        this.requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
        this.streamManager = new ControllerStreamManager(this.requestSender, kafkaConfig);
        this.objectManager = new ControllerObjectManager(this.requestSender, this.metadataManager, kafkaConfig);
        this.blockCache = new DefaultS3BlockCache(this.config.s3CacheSize(), objectManager, operator);
        this.compactionManager = new CompactionManager(this.config, this.objectManager);
        this.writeAheadLog = BlockWALService.builder(this.config.s3WALPath(), this.config.s3WALCapacity()).config(this.config).build();
        this.storage = new S3Storage(this.config, writeAheadLog, streamManager, objectManager, blockCache, operator);
        this.streamClient = new S3StreamClient(this.streamManager, this.storage, this.objectManager, this.operator, this.config);
        this.kvClient = new ControllerKVClient(this.requestSender);
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
        this.storage.shutdown();
        this.streamClient.shutdown();
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
