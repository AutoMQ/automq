package com.automq.stream.s3.operator;

import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class AutoMQStaticCredentialsProvider implements AwsCredentialsProvider {
    // Deprecated. Use AWS_ACCESS_KEY_ID instead.
    private static final String KAFKA_S3_ACCESS_KEY = "KAFKA_S3_ACCESS_KEY";
    // Deprecated. Use AWS_SECRET_ACCESS_KEY instead.
    private static final String KAFKA_S3_SECRET_KEY = "KAFKA_S3_SECRET_KEY";
    private final AwsCredentialsProvider staticCredentialsProvider;

    public AutoMQStaticCredentialsProvider(BucketURI bucketURI) {
        String accessKey = bucketURI.extensionString(BucketURI.ACCESS_KEY_KEY, System.getenv(KAFKA_S3_ACCESS_KEY));
        String secretKey = bucketURI.extensionString(BucketURI.SECRET_KEY_KEY, System.getenv(KAFKA_S3_SECRET_KEY));
        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            staticCredentialsProvider = null;
            return;
        }
        staticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    }

    @Override
    public AwsCredentials resolveCredentials() {
        if (staticCredentialsProvider == null) {
            throw new RuntimeException("AK/SK not set in bucket URI");
        }
        return staticCredentialsProvider.resolveCredentials();
    }
}
