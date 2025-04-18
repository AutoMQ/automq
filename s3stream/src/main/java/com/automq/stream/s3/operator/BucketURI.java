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

package com.automq.stream.s3.operator;

import com.automq.stream.utils.IdURI;
import com.automq.stream.utils.SecretUtils;
import com.automq.stream.utils.URIUtils;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.automq.stream.utils.URIUtils.URI_PATTERN;

public class BucketURI {
    private static final String ENDPOINT_KEY = "endpoint";
    private static final String REGION_KEY = "region";
    public static final String ACCESS_KEY_KEY = "accessKey";
    public static final String SECRET_KEY_KEY = "secretKey";
    private static final String EMPTY_STRING = "";
    private final short bucketId;
    private final String protocol;
    private final String bucket;
    private final String region;
    private String endpoint;
    private final Map<String, List<String>> extension;

    private BucketURI(short bucketId, String protocol, String endpoint, String bucket, String region,
        Map<String, List<String>> extension) {
        this.bucketId = bucketId;
        this.protocol = protocol;
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.region = region;
        this.extension = extension;
        // TODO: extend IdURI
    }

    public static BucketURI parse(String bucketStr) {
        Matcher matcher = URI_PATTERN.matcher(bucketStr);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid bucket url: " + bucketStr);
        }
        short bucketId = Short.parseShort(matcher.group(1));
        String bucket;
        try {
            URI uri = new URI(matcher.group(2));
            String protocol = uri.getScheme();
            String path = uri.getRawSchemeSpecificPart();
            int queryIndex = path.indexOf('?');
            if (queryIndex != -1) {
                path = path.substring(0, queryIndex);
            }
            bucket = path.substring(2);
            Map<String, List<String>> queries = URIUtils.splitQuery(uri);
            String endpoint = URIUtils.getString(queries, ENDPOINT_KEY, EMPTY_STRING);
            queries.remove(ENDPOINT_KEY);
            String region = URIUtils.getString(queries, REGION_KEY, EMPTY_STRING);
            queries.remove(REGION_KEY);
            return new BucketURI(bucketId, protocol, endpoint, bucket, region, queries);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static BucketURI parse(IdURI idURI) {
        Map<String, List<String>> queries = new HashMap<>(idURI.extension());
        queries.remove(ENDPOINT_KEY);
        queries.remove(REGION_KEY);
        return new BucketURI(
            idURI.id(),
            idURI.protocol(),
            idURI.extensionString(ENDPOINT_KEY, EMPTY_STRING),
            idURI.path(),
            idURI.extensionString(REGION_KEY, EMPTY_STRING),
            queries
        );
    }

    public static List<BucketURI> parseBuckets(String bucketsStr) {
        return URIUtils.parseIdURIList(bucketsStr).stream().map(BucketURI::parse).collect(Collectors.toList());
    }

    public short bucketId() {
        return bucketId;
    }

    public String protocol() {
        return protocol;
    }

    public String endpoint() {
        return endpoint;
    }

    public void endpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String bucket() {
        return bucket;
    }

    public String region() {
        return region;
    }

    public String extensionString(String key) {
        return URIUtils.getString(extension, key, null);
    }

    public String extensionString(String key, String defaultVal) {
        return URIUtils.getString(extension, key, defaultVal);
    }

    public List<String> extensionStringList(String key) {
        return URIUtils.getStringList(extension, key);
    }

    public boolean extensionBool(String key, boolean defaultVal) {
        String value = URIUtils.getString(extension, key, null);
        if (StringUtils.isBlank(value)) {
            return defaultVal;
        }
        return Boolean.parseBoolean(value);
    }

    public void addExtension(String key, String value) {
        extension.put(key, List.of(value));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BucketURL{" +
            "bucketId=" + bucketId +
            ", protocol='" + protocol + '\'' +
            ", bucket='" + bucket + '\'' +
            ", region='" + region + '\'' +
            ", endpoint='" + endpoint + '\'');
        sb.append(", extension={");
        extension.forEach((k, v) -> {
            sb.append(k).append("=");
            if (k.equals(SECRET_KEY_KEY)) {
                sb.append(SecretUtils.mask(v.get(0)));
            } else {
                sb.append(v);
            }
            sb.append(", ");
        });
        sb.append("}");
        return sb.toString();
    }
}
