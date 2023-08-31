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

package kafka.log.s3.memory;

import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.KVClient;
import com.automq.elasticstream.client.api.StreamClient;
import kafka.log.es.MemoryClient;
import kafka.log.s3.S3StreamClient;
import kafka.log.s3.S3Wal;
import kafka.log.s3.Wal;
import kafka.log.s3.cache.DefaultS3BlockCache;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.operator.S3Operator;

public class MemoryS3Client implements Client {
    private final StreamClient streamClient;
    private final KVClient kvClient;

    public MemoryS3Client(S3Operator s3Operator) {
        MemoryMetadataManager manager = new MemoryMetadataManager();
        manager.start();
        Wal wal = new S3Wal(manager, s3Operator);
        S3BlockCache blockCache = new DefaultS3BlockCache(manager, s3Operator);
        this.streamClient = new S3StreamClient(manager, wal, blockCache,  manager);
        this.kvClient = new MemoryClient.KVClientImpl();
    }

    @Override
    public StreamClient streamClient() {
        return streamClient;
    }

    @Override
    public KVClient kvClient() {
        return kvClient;
    }
}
