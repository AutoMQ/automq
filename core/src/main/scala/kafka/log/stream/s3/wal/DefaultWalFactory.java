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

package kafka.log.stream.s3.wal;

import com.automq.shell.AutoMQApplication;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.ReservationService;
import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.impl.object.ObjectReservationService;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.IdURI;
import com.automq.stream.utils.Time;

import java.util.Locale;
import java.util.Map;

public class DefaultWalFactory implements WalFactory {
    private final int nodeId;
    private final Map<String, String> objectTagging;
    private final NetworkBandwidthLimiter networkInboundLimiter;
    private final NetworkBandwidthLimiter networkOutboundLimiter;

    public DefaultWalFactory(int nodeId, Map<String, String> objectTagging,
        NetworkBandwidthLimiter networkInboundLimiter, NetworkBandwidthLimiter networkOutboundLimiter) {
        this.nodeId = nodeId;
        this.objectTagging = objectTagging;
        this.networkInboundLimiter = networkInboundLimiter;
        this.networkOutboundLimiter = networkOutboundLimiter;
    }

    @Override
    public WriteAheadLog build(IdURI uri, BuildOptions options) {
        //noinspection SwitchStatementWithTooFewBranches
        switch (uri.protocol().toUpperCase(Locale.ENGLISH)) {
            case "S3":
                BucketURI bucketURI = to(uri);
                ObjectStorage walObjectStorage = ObjectStorageFactory.instance()
                    .builder(bucketURI)
                    .tagging(objectTagging)
                    .inboundLimiter(networkInboundLimiter)
                    .outboundLimiter(networkOutboundLimiter)
                    .build();

                ObjectWALConfig.Builder configBuilder = ObjectWALConfig.builder().withURI(uri)
                    .withClusterId(AutoMQApplication.getClusterId())
                    .withNodeId(nodeId)
                    .withEpoch(options.nodeEpoch())
                    .withOpenMode(options.openMode());
                ReservationService reservationService = new ObjectReservationService(AutoMQApplication.getClusterId(), walObjectStorage, walObjectStorage.bucketId());
                configBuilder.withReservationService(reservationService);
                return new ObjectWALService(Time.SYSTEM, walObjectStorage, configBuilder.build());
            default:
                throw new IllegalArgumentException("Unsupported WAL protocol: " + uri.protocol());
        }
    }

    static BucketURI to(IdURI uri) {
        return BucketURI.parse(uri.encode());
    }
}
