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
