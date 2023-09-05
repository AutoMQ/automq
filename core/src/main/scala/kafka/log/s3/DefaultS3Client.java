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

import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.KVClient;
import com.automq.elasticstream.client.api.StreamClient;
import kafka.log.s3.cache.DefaultS3BlockCache;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.network.ControllerRequestSender;
import kafka.log.s3.objects.ControllerObjectManager;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.streams.ControllerStreamManager;
import kafka.log.s3.streams.StreamManager;
import kafka.log.s3.wal.MemoryWriteAheadLog;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;

public class DefaultS3Client implements Client {

    private final KafkaConfig config;
    private final StreamMetadataManager metadataManager;

    private final ControllerRequestSender requestSender;

    private final S3Operator operator;

    private final Storage storage;

    private final S3BlockCache blockCache;

    private final ObjectManager objectManager;

    private final StreamManager streamManager;

    private final StreamClient streamClient;

    private final KVClient kvClient;

    public DefaultS3Client(BrokerServer brokerServer, KafkaConfig config, S3Operator operator) {
        this.config = config;
        this.metadataManager = new StreamMetadataManager(brokerServer, config);
        this.operator = operator;
        this.requestSender = new ControllerRequestSender(brokerServer);
        this.streamManager = new ControllerStreamManager(this.requestSender, config);
        this.objectManager = new ControllerObjectManager(this.requestSender, this.metadataManager, this.config);
        this.blockCache = new DefaultS3BlockCache(objectManager, operator);
        this.storage = new S3Storage(config, new MemoryWriteAheadLog(), objectManager, blockCache, operator);
        this.streamClient = new S3StreamClient(this.streamManager, this.storage);
        this.kvClient = new ControllerKVClient(this.requestSender);
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
