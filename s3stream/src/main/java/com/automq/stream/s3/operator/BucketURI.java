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

package com.automq.stream.s3.operator;

import com.automq.stream.utils.SecretUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class BucketURI {
    private static final Pattern BUCKETS_URL_PATTERN = Pattern.compile("\\d+@.*?(?=,\\d+@|$)");
    private static final Pattern BUCKET_URL_PATTERN = Pattern.compile("(\\d+)@(.+)");
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
    }

    public static BucketURI parse(String bucketStr) {
        Matcher matcher = BUCKET_URL_PATTERN.matcher(bucketStr);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid bucket url: " + bucketStr);
        }
        short bucketId = Short.parseShort(matcher.group(1));
        try {
            URI uri = new URI(matcher.group(2));
            String protocol = uri.getScheme();
            String bucket = uri.getHost();
            Map<String, List<String>> queries = splitQuery(uri);
            String endpoint = getString(queries, ENDPOINT_KEY, EMPTY_STRING);
            queries.remove(ENDPOINT_KEY);
            String region = getString(queries, REGION_KEY, EMPTY_STRING);
            queries.remove(REGION_KEY);
            return new BucketURI(bucketId, protocol, endpoint, bucket, region, queries);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static List<BucketURI> parseBuckets(String bucketsStr) {
        if (StringUtils.isBlank(bucketsStr)) {
            return Collections.emptyList();
        }
        List<BucketURI> bucketURIList = new ArrayList<>();
        Matcher matcher = BUCKETS_URL_PATTERN.matcher(bucketsStr);
        while (matcher.find()) {
            bucketURIList.add(BucketURI.parse(matcher.group(0)));
        }
        return bucketURIList;
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

    public String extensionString(String key, String defaultVal) {
        return getString(extension, key, defaultVal);
    }

    public List<String> extensionStringList(String key) {
        return getStringList(extension, key);
    }

    public boolean extensionBool(String key, boolean defaultVal) {
        String value = getString(extension, key, null);
        if (StringUtils.isBlank(value)) {
            return defaultVal;
        }
        return Boolean.parseBoolean(value);
    }

    @Override
    public String toString() {
        // TODO: mask the secret info
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

    private static String getString(Map<String, List<String>> queries, String key, String defaultValue) {
        List<String> value = queries.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value.size() > 1) {
            throw new IllegalArgumentException("expect only one value for key: " + key + " but found " + value);
        }
        return value.get(0);
    }

    private static List<String> getStringList(Map<String, List<String>> queries, String key) {
        List<String> value = queries.get(key);
        if (value == null) {
            return Collections.emptyList();
        }
        return value;
    }

    public static Map<String, List<String>> splitQuery(URI uri) {
        if (StringUtils.isBlank(uri.getQuery())) {
            return new HashMap<>();
        }
        final Map<String, List<String>> queryPairs = new LinkedHashMap<>();
        final String[] pairs = uri.getQuery().split("&");
        for (String pair : pairs) {
            final int idx = pair.indexOf("=");
            final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) : pair;
            if (!queryPairs.containsKey(key)) {
                queryPairs.put(key, new LinkedList<>());
            }
            final String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8) : null;
            queryPairs.get(key).add(value);
        }
        return queryPairs;
    }
}
