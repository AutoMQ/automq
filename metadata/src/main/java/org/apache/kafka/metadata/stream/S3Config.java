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

package org.apache.kafka.metadata.stream;

/**
 * S3Config contains the configuration of S3, such as the bucket name, the region, etc.
 */
public class S3Config {

    // Only for test, if true, use mocked S3 related classes
    private final boolean mock;

    private final long objectRetentionTimeInSecond;

    private final String endpoint;

    private final String region;

    private final String bucket;

    // Only for test
    public S3Config(final boolean mock) {
        this(null, null, null, -1, mock);
    }

    public S3Config(final String endpoint, final String region, final String bucket, final long objectRetentionTimeInSecond) {
        this(endpoint, region, bucket, objectRetentionTimeInSecond, false);
    }

    public S3Config(final String endpoint, final String region, final String bucket, final long objectRetentionTimeInSecond,
        final boolean mock) {
        this.endpoint = endpoint;
        this.region = region;
        this.bucket = bucket;
        this.objectRetentionTimeInSecond = objectRetentionTimeInSecond;
        this.mock = mock;
    }

    public String region() {
        return region;
    }

    public String bucket() {
        return bucket;
    }

    public String endpoint() {
        return endpoint;
    }

    public boolean mock() {
        return mock;
    }

    public long objectRetentionTimeInSecond() {
        return objectRetentionTimeInSecond;
    }
}
