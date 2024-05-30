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
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import java.util.List;

public class StreamClient {
    private final Config streamConfig;
    private final S3Operator s3Operator;

    public StreamClient(Config streamConfig) {
        this.streamConfig = streamConfig;
        this.s3Operator = new DefaultS3Operator(
            streamConfig.endpoint(),
            streamConfig.region(),
            streamConfig.bucket(),
            streamConfig.forcePathStyle(),
            List.of(CredentialsProviderHolder.getAwsCredentialsProvider(), EnvVariableCredentialsProvider.get()),
            streamConfig.objectTagging());
    }

    public Config streamConfig() {
        return streamConfig;
    }

    public S3Operator s3Operator() {
        return s3Operator;
    }
}
