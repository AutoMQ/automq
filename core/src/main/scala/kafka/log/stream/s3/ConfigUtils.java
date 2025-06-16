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

package kafka.log.stream.s3;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import com.automq.stream.s3.Config;

public class ConfigUtils {

    public static Config to(KafkaConfig s) {
        AutoMQConfig config = s.automq();
        return new Config()
            .nodeId(s.nodeId())
            .nodeEpoch(config.nodeEpoch())
            .dataBuckets(config.dataBuckets())
            .walConfig(config.walConfig())
            .walCacheSize(s.s3WALCacheSize())
            .walUploadThreshold(s.s3WALUploadThreshold())
            .walUploadIntervalMs(s.s3WALUploadIntervalMs())
            .streamSplitSize(s.s3StreamSplitSize())
            .objectBlockSize(s.s3ObjectBlockSize())
            .objectPartSize(s.s3ObjectPartSize())
            .blockCacheSize(s.s3BlockCacheSize())
            .streamObjectCompactionIntervalMinutes(s.s3StreamObjectCompactionTaskIntervalMinutes())
            .streamObjectCompactionMaxSizeBytes(s.s3StreamObjectCompactionMaxSizeBytes())
            .controllerRequestRetryMaxCount(s.s3ControllerRequestRetryMaxCount())
            .controllerRequestRetryBaseDelayMs(s.s3ControllerRequestRetryBaseDelayMs())
            .streamSetObjectCompactionInterval(s.s3StreamSetObjectCompactionInterval())
            .streamSetObjectCompactionCacheSize(s.s3StreamSetObjectCompactionCacheSize())
            .maxStreamNumPerStreamSetObject(s.s3MaxStreamNumPerStreamSetObject())
            .maxStreamObjectNumPerCommit(s.s3MaxStreamObjectNumPerCommit())
            .streamSetObjectCompactionStreamSplitSize(s.s3StreamSetObjectCompactionStreamSplitSize())
            .streamSetObjectCompactionForceSplitPeriod(s.s3StreamSetObjectCompactionForceSplitMinutes())
            .streamSetObjectCompactionMaxObjectNum(s.s3StreamSetObjectCompactionMaxObjectNum())
            .mockEnable(s.s3MockEnable())
            .networkBaselineBandwidth(s.s3NetworkBaselineBandwidthProp())
            .refillPeriodMs(s.s3RefillPeriodMsProp())
            .objectRetentionTimeInSecond(s.s3ObjectDeleteRetentionTimeInSecond());
    }
}
