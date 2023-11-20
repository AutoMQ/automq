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

package kafka.log.stream.s3;

import com.automq.stream.s3.Config;
import kafka.server.KafkaConfig;

public class ConfigUtils {

    public static Config to(KafkaConfig s) {
        return new Config()
                .nodeId(s.nodeId())
                .endpoint(s.s3Endpoint())
                .region(s.s3Region())
                .bucket(s.s3Bucket())
                .walPath(s.s3WALPath())
                .walCacheSize(s.s3WALCacheSize())
                .walCapacity(s.s3WALCapacity())
                .walHeaderFlushIntervalSeconds(s.s3WALHeaderFlushIntervalSeconds())
                .walThread(s.s3WALThread())
                .walWindowInitial(s.s3WALWindowInitial())
                .walWindowIncrement(s.s3WALWindowIncrement())
                .walWindowMax(s.s3WALWindowMax())
                .walUploadThreshold(s.s3WALUploadThreshold())
                .streamSplitSize(s.s3StreamSplitSize())
                .objectBlockSize(s.s3ObjectBlockSize())
                .objectPartSize(s.s3ObjectPartSize())
                .blockCacheSize(s.s3BlockCacheSize())
                .streamObjectCompactionIntervalMinutes(s.s3StreamObjectCompactionTaskIntervalMinutes())
                .streamObjectCompactionMaxSizeBytes(s.s3StreamObjectCompactionMaxSizeBytes())
                .streamObjectCompactionLivingTimeMinutes(s.s3StreamObjectCompactionLivingTimeMinutes())
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
                .objectRetentionTimeInSecond(s.s3ObjectRetentionTimeInSecond())
                .failoverEnable(s.s3FailoverEnable());
    }

}
