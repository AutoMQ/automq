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

package kafka.controller.streamaspect.client.s3;

import kafka.controller.streamaspect.client.Context;
import kafka.controller.streamaspect.client.StreamClientFactoryProxy;
import kafka.log.stream.s3.ConfigUtils;

import org.apache.kafka.controller.stream.StreamClient;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_BACKGROUND;
import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_KEY;

public class StreamClientFactory {

    /**
     * This method will be called by {@link StreamClientFactoryProxy}
     */
    public static StreamClient get(Context context) {
        Config streamConfig = ConfigUtils.to(context.kafkaConfig);
        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder()
            .buckets(streamConfig.dataBuckets())
            .tagging(streamConfig.objectTagging())
            .extension(EXTENSION_TYPE_KEY, EXTENSION_TYPE_BACKGROUND)
            .build();
        return StreamClient.builder()
            .streamConfig(streamConfig)
            .objectStorage(objectStorage)
            .build();
    }
}
