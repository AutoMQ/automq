/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.utils.IdURI;

import java.util.List;

/** Validates object WAL write layouts against finalized cluster capabilities before broker WAL initialization. */
public final class WALShardingConfigValidator {
    private WALShardingConfigValidator() {
    }

    /**
     * Rejects sharded writes on clusters without compatible readers. Explicit r mode permits a rolling upgrade.
     * Local WALs and read-only router buckets do not introduce sharded objects and are exempt.
     * Call after the initial metadata image is available and before starting any object WAL writers.
     */
    public static void validate(AutoMQConfig config, AutoMQVersion version) {
        if (version.isWALShardingSupported()) {
            return;
        }
        if (config.walConfig() != null) {
            IdURI wal = IdURI.parse(config.walConfig());
            if ("s3".equalsIgnoreCase(wal.protocol())) {
                validateLegacyWrites(wal, AutoMQConfig.S3_WAL_PATH_CONFIG);
            }
        }
        for (BucketURI bucket : config.zoneRouterChannels().orElse(List.of())) {
            if (!"r".equals(bucket.extensionString("mode", "rw"))) {
                validateLegacyWrites(bucket.toIdURI(), AutoMQConfig.ZONE_ROUTER_CHANNELS_CONFIG + " bucket " + bucket.bucketId());
            }
        }
    }

    private static void validateLegacyWrites(IdURI uri, String setting) {
        ObjectWALConfig config = ObjectWALConfig.builder().withURI(uri).build();
        if (!"r".equals(config.shardMode())) {
            throw new ConfigException(setting + " requires explicit shardMode=r while automq.version is below "
                + AutoMQVersion.V6.featureLevel() + ". Upgrade all brokers and controllers, finalize automq.version="
                + AutoMQVersion.V6.featureLevel() + ", then enable shardMode=rw.");
        }
    }
}
