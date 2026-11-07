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

package kafka.automq.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.Optional;

public class JsonUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String toArray(String s) {
        ArrayNode array = MAPPER.createArrayNode();
        array.add(s);
        try {
            return MAPPER.writeValueAsString(array);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Failed to convert %s to json array", s), e);
        }
    }

    public static String encode(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> T decode(String raw, Class<T> clazz) {
        try {
            return MAPPER.readValue(raw, clazz);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("json parse (" + raw + ") fail", e);
        }
    }

    public static Optional<String> getValue(String json, String key) {
        try {
            return Optional.ofNullable(MAPPER.readTree(json).get(key)).map(JsonNode::asText);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("json parse (" + json + ") fail", e);
        }
    }
}
