/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
