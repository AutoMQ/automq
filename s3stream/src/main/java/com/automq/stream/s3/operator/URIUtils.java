/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class URIUtils {
    public static String getString(Map<String, List<String>> queries, String key, String defaultValue) {
        List<String> value = queries.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value.size() > 1) {
            throw new IllegalArgumentException("expect only one value for key: " + key + " but found " + value);
        }
        return value.get(0);
    }

    public static List<String> getStringList(Map<String, List<String>> queries, String key) {
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
