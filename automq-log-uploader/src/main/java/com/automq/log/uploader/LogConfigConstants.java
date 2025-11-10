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

package com.automq.log.uploader;

public class LogConfigConstants {
    private LogConfigConstants() {
    }

    public static final String LOG_PROPERTIES_FILE = "automq-log.properties";

    public static final String LOG_S3_ENABLE_KEY = "log.s3.enable";
    public static final boolean DEFAULT_LOG_S3_ENABLE = false;

    public static final String LOG_S3_BUCKET_KEY = "log.s3.bucket";
    public static final String LOG_S3_REGION_KEY = "log.s3.region";
    public static final String LOG_S3_ENDPOINT_KEY = "log.s3.endpoint";

    public static final String LOG_S3_ACCESS_KEY = "log.s3.access.key";
    public static final String LOG_S3_SECRET_KEY = "log.s3.secret.key";

    public static final String LOG_S3_CLUSTER_ID_KEY = "log.s3.cluster.id";
    public static final String DEFAULT_LOG_S3_CLUSTER_ID = "automq-cluster";

    public static final String LOG_S3_NODE_ID_KEY = "log.s3.node.id";
    public static final int DEFAULT_LOG_S3_NODE_ID = 0;

    public static final String LOG_S3_PRIMARY_NODE_KEY = "log.s3.primary.node";
    public static final String LOG_S3_SELECTOR_TYPE_KEY = "log.s3.selector.type";
    public static final String LOG_S3_SELECTOR_PREFIX = "log.s3.selector.";
}
