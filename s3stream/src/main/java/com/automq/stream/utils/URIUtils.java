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
