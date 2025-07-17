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

package kafka.automq.table;

import com.automq.stream.s3.operator.AutoMQStaticCredentialsProvider;
import com.automq.stream.s3.operator.BucketURI;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;


public class CredentialProviderHolder implements AwsCredentialsProvider {
    private static Function<BucketURI, AwsCredentialsProvider> providerSupplier = bucketURI -> newCredentialsProviderChain(
        credentialsProviders(bucketURI));
    private static BucketURI bucketURI;

    public static void setup(Function<BucketURI, AwsCredentialsProvider> providerSupplier) {
        CredentialProviderHolder.providerSupplier = providerSupplier;
    }

    public static void setup(BucketURI bucketURI) {
        CredentialProviderHolder.bucketURI = bucketURI;
    }

    private static List<AwsCredentialsProvider> credentialsProviders(BucketURI bucketURI) {
        return List.of(new AutoMQStaticCredentialsProvider(bucketURI), DefaultCredentialsProvider.builder().build());
    }

    private static AwsCredentialsProvider newCredentialsProviderChain(
        List<AwsCredentialsProvider> credentialsProviders) {
        List<AwsCredentialsProvider> providers = new ArrayList<>(credentialsProviders);
        providers.add(AnonymousCredentialsProvider.create());
        return AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(providers)
            .build();
    }

    // iceberg will invoke create with reflection.
    public static AwsCredentialsProvider create() {
        if (bucketURI == null) {
            throw new IllegalStateException("BucketURI must be set before calling create(). Please invoke setup(BucketURI) first.");
        }
        return providerSupplier.apply(bucketURI);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        throw new UnsupportedOperationException();
    }
}
