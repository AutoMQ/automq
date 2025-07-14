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
package com.automq.shell.model;

import software.amazon.awssdk.utils.StringUtils;

public class S3Url {

    final String s3AccessKey;
    final String s3SecretKey;

    final String s3Region;

    final EndpointProtocol endpointProtocol;

    final String s3Endpoint;

    final String s3DataBucket;

    final String s3OpsBucket;

    final String clusterId;

    final boolean s3PathStyle;

    public S3Url(String s3AccessKey, String s3SecretKey, String s3Region,
        EndpointProtocol endpointProtocol, String s3Endpoint, String s3DataBucket, String s3OpsBucket, String clusterId,
        boolean s3PathStyle) {
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.s3Region = s3Region;
        this.endpointProtocol = endpointProtocol;
        this.s3Endpoint = s3Endpoint;
        this.s3DataBucket = s3DataBucket;
        this.s3OpsBucket = s3OpsBucket;
        this.clusterId = clusterId;
        this.s3PathStyle = s3PathStyle;
    }

    /**
     * @param args input args to start AutoMQ
     * @return s3Url value from args, or null if not found
     */
    public static String parseS3UrlValFromArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--s3-url=")) {
                return arg.substring("--s3-url=".length());
            }
        }
        return null;
    }

    public static S3Url parse(String s3Url) throws IllegalArgumentException {
        if (StringUtils.isBlank(s3Url)) {
            throw new IllegalArgumentException("s3Url required");
        }
        // skip the first prefix "s3://"
        String s3Endpoint = s3Url.substring(5, s3Url.indexOf('?'));

        String paramsPart = s3Url.substring(s3Url.indexOf('?') + 1);
        String[] params = paramsPart.split("&");

        String accessKey = null;
        String secretKey = null;
        String region = null;
        EndpointProtocol protocol = null;
        String dataBucket = null;
        String opsBucket = null;
        String clusterId = null;
        boolean s3PathStyle = false;

        for (String param : params) {
            String[] keyValue = param.split("=");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid parameter format: " + param);
            }

            String key = keyValue[0];
            String value = keyValue[1];

            switch (key) {
                case "s3-access-key":
                    accessKey = value;
                    break;
                case "s3-secret-key":
                    secretKey = value;
                    break;
                case "s3-region":
                    region = value;
                    break;
                case "s3-endpoint-protocol":
                    protocol = EndpointProtocol.getByName(value);
                    break;
                case "s3-data-bucket":
                    dataBucket = value;
                    break;
                case "s3-ops-bucket":
                    opsBucket = value;
                    break;
                case "cluster-id":
                    clusterId = value;
                    break;
                case "s3-path-style":
                    s3PathStyle = Boolean.parseBoolean(value);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown parameter: " + key);
            }
        }

        return new S3Url(accessKey, secretKey, region, protocol, s3Endpoint, dataBucket, opsBucket, clusterId, s3PathStyle);
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public String getS3Region() {
        return s3Region;
    }

    public EndpointProtocol getEndpointProtocol() {
        return endpointProtocol;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3DataBucket() {
        return s3DataBucket;
    }

    public String getS3OpsBucket() {
        return s3OpsBucket;
    }

    public String getClusterId() {
        return clusterId;
    }

    public boolean isS3PathStyle() {
        return s3PathStyle;
    }
}
