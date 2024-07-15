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

package kafka.controller.streamaspect.client.s3;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import kafka.controller.streamaspect.client.Context;
import kafka.controller.streamaspect.client.StreamClientFactoryProxy;
import kafka.log.stream.s3.ConfigUtils;
import org.apache.kafka.controller.stream.StreamClient;

public class StreamClientFactory {

    /**
     * This method will be called by {@link StreamClientFactoryProxy}
     */
    public static StreamClient get(Context context) {
        Config streamConfig = ConfigUtils.to(context.kafkaConfig);
        return StreamClient.builder()
            .streamConfig(streamConfig)
            .objectStorage(
                ObjectStorageFactory.instance()
                    .builder(streamConfig.dataBuckets().get(0))
                    .tagging(streamConfig.objectTagging())
                    .build()
            )
            .build();
    }
}
