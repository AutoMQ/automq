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

package kafka.server.streamaspect.client.s3;

import com.automq.shell.auth.CredentialsProviderHolder;
import com.automq.shell.auth.EnvVariableCredentialsProvider;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import java.util.List;
import kafka.log.stream.s3.ConfigUtils;
import kafka.server.streamaspect.client.Context;
import org.apache.kafka.controller.stream.StreamClient;

public class StreamClientFactory {
    public static StreamClient get(Context context) {
        Config streamConfig = ConfigUtils.to(context.kafkaConfig);
        S3Operator s3Operator = new DefaultS3Operator(
            streamConfig.endpoint(),
            streamConfig.region(),
            streamConfig.bucket(),
            streamConfig.forcePathStyle(),
            List.of(CredentialsProviderHolder.getAwsCredentialsProvider(), EnvVariableCredentialsProvider.get()),
            streamConfig.objectTagging());
        return new StreamClient(streamConfig, s3Operator);
    }
}
