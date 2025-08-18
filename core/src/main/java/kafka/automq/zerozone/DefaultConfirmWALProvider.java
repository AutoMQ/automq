/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.zerozone;

import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.GlobalNetworkBandwidthLimiters;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.Time;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultConfirmWALProvider implements ConfirmWALProvider {
    private final Map<Short, ObjectStorage> objectStorages = new ConcurrentHashMap<>();
    private final String clusterId;
    private final Time time = Time.SYSTEM;

    public DefaultConfirmWALProvider(String clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public WriteAheadLog readOnly(String walConfig, int nodeId) {
        BucketURI bucketURI = BucketURI.parse(walConfig);
        ObjectStorage objectStorage = objectStorages.computeIfAbsent(bucketURI.bucketId(), id -> {
                try {
                    return ObjectStorageFactory.instance().builder(bucketURI).readWriteIsolate(false)
                        .inboundLimiter(GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND))
                        .outboundLimiter(GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.OUTBOUND))
                        .build();
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }
        );
        if (objectStorage == null) {
            throw new IllegalArgumentException("Cannot parse " + walConfig);
        }
        ObjectWALConfig objectWALConfig = ObjectWALConfig.builder()
            .withClusterId(clusterId)
            .withNodeId(nodeId)
            .withOpenMode(OpenMode.READ_ONLY)
            .build();
        return new ObjectWALService(time, objectStorage, objectWALConfig);
    }
}
