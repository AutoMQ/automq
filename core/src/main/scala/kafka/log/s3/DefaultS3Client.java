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

package kafka.log.s3;

import kafka.log.es.api.Client;
import kafka.log.es.api.KVClient;
import kafka.log.es.api.StreamClient;
import kafka.log.s3.cache.DefaultS3BlockCache;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.compact.CompactionManager;
import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.network.ControllerRequestSender;
import kafka.log.s3.network.ControllerRequestSender.RetryPolicyContext;
import kafka.log.s3.objects.ControllerObjectManager;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.streams.ControllerStreamManager;
import kafka.log.s3.streams.StreamManager;
import kafka.log.s3.wal.BlockWALService;
import kafka.log.s3.wal.WriteAheadLog;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;

public class DefaultS3Client implements Client {

    private final KafkaConfig config;
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

    public DefaultS3Client(BrokerServer brokerServer, KafkaConfig config, S3Operator operator) {
        this.config = config;
        this.metadataManager = new StreamMetadataManager(brokerServer, config);
        this.operator = operator;
        RetryPolicyContext retryPolicyContext = new RetryPolicyContext(config.s3ControllerRequestRetryMaxCount(),
                config.s3ControllerRequestRetryBaseDelayMs());
        this.requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
        this.streamManager = new ControllerStreamManager(this.requestSender, config);
        this.objectManager = new ControllerObjectManager(this.requestSender, this.metadataManager, this.config);
        this.blockCache = new DefaultS3BlockCache(config.s3CacheSize(), objectManager, operator);
        this.compactionManager = new CompactionManager(this.config, this.objectManager, this.operator);
        this.compactionManager.start();
        this.writeAheadLog = BlockWALService.builder(config.s3WALPath()).config(config).build();
        this.storage = new S3Storage(config, writeAheadLog, streamManager, objectManager, blockCache, operator);
        this.storage.startup();
        this.streamClient = new S3StreamClient(this.streamManager, this.storage, this.objectManager, this.operator, this.config);
        this.kvClient = new ControllerKVClient(this.requestSender);
        // TODO: startup method
    }

    @Override
    public StreamClient streamClient() {
        return this.streamClient;
    }

    @Override
    public KVClient kvClient() {
        return this.kvClient;
    }

    public void shutdown() {
        this.storage.shutdown();
        this.compactionManager.shutdown();
        this.streamClient.shutdown();
    }
}
