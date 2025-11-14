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

package org.apache.kafka.connect.automq.log;

import org.apache.kafka.connect.automq.runtime.LeaderNodeSelector;
import org.apache.kafka.connect.automq.runtime.RuntimeLeaderSelectorProvider;

import com.automq.log.uploader.S3LogConfig;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectS3LogConfig implements S3LogConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectS3LogConfig.class);

    private final boolean enable;
    private final String clusterId;
    private final int nodeId;
    private final String bucketURI;
    private ObjectStorage objectStorage;
    private LeaderNodeSelector leaderNodeSelector;
    
    
    public ConnectS3LogConfig(boolean enable, String clusterId, int nodeId, String bucketURI) {
        this.enable = enable;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.bucketURI = bucketURI;
    }

    @Override
    public boolean isEnabled() {
        return this.enable;
    }

    @Override
    public String clusterId() {
        return this.clusterId;
    }

    @Override
    public int nodeId() {
        return this.nodeId;
    }

    @Override
    public synchronized ObjectStorage objectStorage() {
        if (this.objectStorage != null) {
            return this.objectStorage;
        }
        if (StringUtils.isBlank(bucketURI)) {
            LOGGER.error("Mandatory log config bucketURI is not set.");
            return null;
        }

        String normalizedBucket = bucketURI.trim();
        BucketURI logBucket = BucketURI.parse(normalizedBucket);
        this.objectStorage = ObjectStorageFactory.instance().builder(logBucket).threadPrefix("s3-log-uploader").build();
        return this.objectStorage;
    }

    @Override
    public boolean isLeader() {
        LeaderNodeSelector selector = leaderSelector();
        return selector != null && selector.isLeader();
    }
    
    public LeaderNodeSelector leaderSelector() {
        if (leaderNodeSelector == null) {
            this.leaderNodeSelector = new RuntimeLeaderSelectorProvider().createSelector();
        }
        return leaderNodeSelector;
    }
}
