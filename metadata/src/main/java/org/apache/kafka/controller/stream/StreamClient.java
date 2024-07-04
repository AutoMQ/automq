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

package org.apache.kafka.controller.stream;

import com.automq.shell.auth.CredentialsProviderHolder;
import com.automq.shell.auth.EnvVariableCredentialsProvider;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import java.util.List;

public class StreamClient {
    private final Config streamConfig;
    private final ObjectStorage objectStorage;

    public StreamClient(Config streamConfig) {
        this.streamConfig = streamConfig;
        this.objectStorage = AwsObjectStorage.builder()
            .bucket(streamConfig.dataBuckets().get(0))
            .credentialsProviders(List.of(CredentialsProviderHolder.getAwsCredentialsProvider(), EnvVariableCredentialsProvider.get()))
            .tagging(streamConfig.objectTagging())
            .build();
    }

    public Config streamConfig() {
        return streamConfig;
    }

    public ObjectStorage objectStorage() {
        return objectStorage;
    }
}
