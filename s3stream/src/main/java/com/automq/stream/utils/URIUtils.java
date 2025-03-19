/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.utils;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
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

public class URIUtils {
    public static final Pattern URI_LIST_PATTERN = Pattern.compile("[-]?\\d+@.*?(?=,[-]?\\d+@|$)");
    public static final Pattern URI_PATTERN = Pattern.compile("([-]?\\d+)@(.+)");

    public static List<String> parseIdURIList(String uriList) {
        if (StringUtils.isBlank(uriList)) {
            return Collections.emptyList();
        }
        List<String> list = new ArrayList<>();
        Matcher matcher = URI_LIST_PATTERN.matcher(uriList);
        while (matcher.find()) {
            list.add(matcher.group(0));
        }
        return list;
    }

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
        String rawQuery = uri.getRawQuery();
        return splitQuery(rawQuery);
    }

    public static Map<String, List<String>> splitQuery(String rawQuery) {
        if (StringUtils.isBlank(rawQuery)) {
            return new HashMap<>();
        }
        final Map<String, List<String>> queryPairs = new LinkedHashMap<>();
        final String[] pairs = rawQuery.split("&");
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
