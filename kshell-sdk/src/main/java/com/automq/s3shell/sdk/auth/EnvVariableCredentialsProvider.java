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
package com.automq.s3shell.sdk.auth;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class EnvVariableCredentialsProvider {
    public static final String ACCESS_KEY_NAME = "KAFKA_S3_ACCESS_KEY";
    public static final String SECRET_KEY_NAME = "KAFKA_S3_SECRET_KEY";

    public static AwsCredentialsProvider get() {
        String accessKey = System.getenv(ACCESS_KEY_NAME);
        String secretKey = System.getenv(SECRET_KEY_NAME);
        return () -> AwsBasicCredentials.create(accessKey, secretKey);
    }

    @Override
    public String toString() {
        return "EnvVariableCredentialsProvider";
    }
}
