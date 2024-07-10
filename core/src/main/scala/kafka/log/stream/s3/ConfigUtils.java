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
import com.google.common.base.Splitter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

public class ConfigUtils {

    public static Config to(KafkaConfig s) {
        AutoMQConfig config = s.automq();
        return new Config()
            .nodeId(s.nodeId())
            .dataBuckets(config.dataBuckets())
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
            .objectRetentionTimeInSecond(s.s3ObjectDeleteRetentionTimeInSecond());
    }

    public static WALConfig toWALConfig(String walPathConfig) {
        if (!walPathConfig.startsWith("0@")) {
            return new WALConfig("file", walPathConfig, Collections.emptyMap());
        }

        if (!walPathConfig.contains(":")) {
            throw new IllegalArgumentException("Invalid WAL path: " + walPathConfig);
        }
        String schema = walPathConfig.substring(2, walPathConfig.indexOf(":"));

        int parameterIndex = walPathConfig.indexOf("?");
        if (parameterIndex == -1) {
            return new WALConfig(schema, walPathConfig.substring(2 + schema.length() + 3), Collections.emptyMap());
        }

        String target = walPathConfig.substring(2 + schema.length() + 3, parameterIndex);
        Map<String, String> parameterMap = Splitter.on("&")
            .withKeyValueSeparator("=")
            .split(walPathConfig.substring(parameterIndex + 1));
        return new WALConfig(schema, target, parameterMap);
    }

    public static class WALConfig {
        private final String schema;
        private final String target;
        private final Map<String, String> parameterMap;

        public WALConfig(String schema, String target, Map<String, String> parameterMap) {
            this.schema = schema;
            this.target = target;
            this.parameterMap = parameterMap;
        }

        public String schema() {
            return schema;
        }

        public String target() {
            return target;
        }

        public Optional<String> parameter(String key) {
            if (parameterMap == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(parameterMap.get(key));
        }
    }

}
