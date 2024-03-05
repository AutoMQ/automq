/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3;

import com.automq.stream.s3.Config;
import kafka.server.KafkaConfig;

public class ConfigUtils {

    public static Config to(KafkaConfig s) {
        return new Config()
                .nodeId(s.nodeId())
                .nodeEpoch(s.nodeEpoch())
                .endpoint(s.s3Endpoint())
                .region(s.s3Region())
                .bucket(s.s3Bucket())
                .walPath(s.s3WALPath())
                .walCacheSize(s.s3WALCacheSize())
                .walCapacity(s.s3WALCapacity())
                .walThread(s.s3WALThread())
                .walWriteRateLimit(s.s3WALIOPS())
                .walUploadThreshold(s.s3WALUploadThreshold())
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
                .objectLogEnable(s.s3ObjectLogEnable())
                .networkBaselineBandwidth(s.s3NetworkBaselineBandwidthProp())
                .refillPeriodMs(s.s3RefillPeriodMsProp())
                .objectRetentionTimeInSecond(s.s3ObjectDeleteRetentionTimeInSecond())
                .forcePathStyle(s.s3PathStyle());
    }

}
