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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class CredentialsProviderHolder {

    //global singleton credential provider
    private static volatile AwsCredentialsProvider awsCredentialsProvider;

    private CredentialsProviderHolder() {
    }

    public static AwsCredentialsProvider getAwsCredentialsProvider() {
        if (awsCredentialsProvider == null) {
            throw new IllegalStateException("AwsCredentialsProvider has not been initialized");
        }
        return awsCredentialsProvider;
    }

    public static synchronized void create(AwsCredentialsProvider newAwsCredentialsProvider) {
        if (awsCredentialsProvider != null) {
            throw new IllegalStateException("AwsCredentialsProvider is already initialized");
        }
        awsCredentialsProvider = newAwsCredentialsProvider;
    }
}
