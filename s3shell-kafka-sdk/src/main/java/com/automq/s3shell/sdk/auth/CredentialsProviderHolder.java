/*
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
package com.automq.s3shell.sdk.auth;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class CredentialsProviderHolder {

    //global singleton credential provider
    private static volatile AwsCredentialsProvider awsCredentialsProvider;

    private CredentialsProviderHolder() {
    }

    static {
        // TODO: When we have a way to pass in credentials, we should remove this
        create(EnvVariableCredentialsProvider.get());
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
