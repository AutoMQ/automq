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
package com.automq.shell.auth;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class EnvVariableCredentialsProvider {
    public static final String ACCESS_KEY_NAME = "KAFKA_S3_ACCESS_KEY";
    public static final String SECRET_KEY_NAME = "KAFKA_S3_SECRET_KEY";

    public static AwsCredentialsProvider get() {
        return EnvVariableCredentialsProvider::create;
    }

    private static AwsCredentials create() {
        String accessKey = System.getenv(ACCESS_KEY_NAME);
        String secretKey = System.getenv(SECRET_KEY_NAME);
        // According to the AWS documentation, when we fail to get the credentials in a credential provider,
        // we should throw a RuntimeException to indicate that the provider is not able to provide the credential.
        if (accessKey == null) {
            throw new NullPointerException("Access key is not set");
        }
        if (secretKey == null) {
            throw new NullPointerException("Secret key is not set");
        }
        return AwsBasicCredentials.create(accessKey, secretKey);
    }

    @Override
    public String toString() {
        return "EnvVariableCredentialsProvider";
    }
}
