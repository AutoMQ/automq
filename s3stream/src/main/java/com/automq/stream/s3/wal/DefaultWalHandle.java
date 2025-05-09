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

package com.automq.stream.s3.wal;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.impl.object.ObjectReservationService;
import com.automq.stream.utils.IdURI;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;

public class DefaultWalHandle implements WalHandle {

    private final String clusterId;

    public DefaultWalHandle(String clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public CompletableFuture<Void> acquirePermission(int nodeId, long nodeEpoch, IdURI walConfig,
        AcquirePermissionOptions options) {
        //noinspection SwitchStatementWithTooFewBranches
        switch (walConfig.protocol().toUpperCase(Locale.ENGLISH)) {
            case "S3": {
                return acquireObjectWALPermission(nodeId, nodeEpoch, walConfig, options);
            }
            default: {
                throw new IllegalArgumentException(String.format("Unsupported WAL protocol %s in %s", walConfig.protocol(), walConfig));
            }
        }
    }

    @Override
    public CompletableFuture<Void> releasePermission(IdURI walConfig, ReleasePermissionOptions options) {
        //noinspection SwitchStatementWithTooFewBranches
        switch (walConfig.protocol().toUpperCase(Locale.ENGLISH)) {
            case "S3": {
                return CompletableFuture.completedFuture(null);
            }
            default: {
                throw new IllegalArgumentException(String.format("Unsupported WAL protocol %s in %s", walConfig.protocol(), walConfig));
            }
        }
    }

    private CompletableFuture<Void> acquireObjectWALPermission(int nodeId, long nodeEpoch, IdURI walConfig,
        AcquirePermissionOptions options) {
        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(BucketURI.parse(walConfig)).build();
        ObjectReservationService reservationService = new ObjectReservationService(clusterId, objectStorage, walConfig.id());
        return reservationService.acquire(nodeId, nodeEpoch, options.failoverMode());
    }
}
