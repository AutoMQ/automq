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
