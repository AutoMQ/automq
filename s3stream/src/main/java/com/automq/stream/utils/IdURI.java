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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static com.automq.stream.utils.URIUtils.URI_PATTERN;

public class IdURI {
    private final short id;
    private final String protocol;
    private final String path;
    private final Map<String, List<String>> extension;

    private IdURI(short id, URI uri) {
        this.id = id;

        this.protocol = uri.getScheme();
        String path = uri.getRawSchemeSpecificPart();
        int queryIndex = path.indexOf('?');
        if (queryIndex != -1) {
            path = path.substring(0, queryIndex);
        }
        this.path = path.substring(2);
        this.extension = URIUtils.splitQuery(uri);
    }

    private IdURI(short id, String protocol, String path, Map<String, List<String>> extension) {
        this.id = id;
        this.protocol = protocol;
        this.path = path;
        this.extension = extension;
    }

    public static IdURI parse(String raw) {
        Matcher matcher = URI_PATTERN.matcher(raw);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid URI: " + raw);
        }
        short bucketId = Short.parseShort(matcher.group(1));
        try {
            URI uri = new URI(matcher.group(2));
            return new IdURI(bucketId, uri);
        } catch (Throwable e) {
            throw new IllegalArgumentException(e);
        }
    }

    public short id() {
        return id;
    }

    public String protocol() {
        return protocol;
    }

    public String path() {
        return path;
    }

    public IdURI path(String newPath) {
        return new IdURI(id, protocol, newPath, extension);
    }

    public Map<String, List<String>> extension() {
        return Collections.unmodifiableMap(extension);
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

    public long extensionLong(String key, long defaultVal) {
        String value = URIUtils.getString(extension, key, null);
        if (StringUtils.isBlank(value)) {
            return defaultVal;
        }
        return Long.parseLong(value);
    }

    public String encode() {
        StringBuilder raw = new StringBuilder(id() + "@" + protocol() + "://" + path());
        if (extension.isEmpty()) {
            return raw.toString();
        }
        raw.append("?");
        for (Map.Entry<String, List<String>> entry : extension().entrySet()) {
            for (String value : entry.getValue()) {
                raw.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)).append("=").append(URLEncoder.encode(value, StandardCharsets.UTF_8)).append("&");
            }
        }
        return raw.substring(0, raw.length() - 1);
    }

    @Override
    public String toString() {
        return encode();
    }
}
