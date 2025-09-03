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

package org.apache.kafka.server.common.automq;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

public class TableTopicConfigValidator {
    static final Pattern LIST_STRING_REGEX = Pattern.compile("\\[(.*)\\]");
    public static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";
    private static final Pattern COLUMN_NAME_REGEX = Pattern.compile("[a-zA-Z0-9_\\-\\.]+");

    public static class PartitionValidator implements ConfigDef.Validator {
        public static final PartitionValidator INSTANCE = new PartitionValidator();
        private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return;
            }
            try {
                String str = (String) value;
                List<String> partitions = parsePartitionBy(str);
                partitions.forEach(partitionField -> {
                    Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
                    if (matcher.matches()) {
                        Transform transform = Transform.fromString(matcher.group(1));
                        switch (transform) {
                            case YEAR:
                            case MONTH:
                            case DAY:
                            case HOUR:
                                String columnName = matcher.group(2);
                                if (!COLUMN_NAME_REGEX.matcher(columnName).matches()) {
                                    throw new ConfigException(name, value, String.format("Invalid column name %s", columnName));
                                }
                                break;
                            case BUCKET:
                            case TRUNCATE:
                                Pair<String, Integer> pair = transformArgPair(matcher.group(2));
                                if (!COLUMN_NAME_REGEX.matcher(pair.getLeft()).matches()) {
                                    throw new ConfigException(name, value, String.format("Invalid column name %s", pair.getLeft()));
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("Unsupported transform: " + transform);
                        }
                    } else {
                        if (!COLUMN_NAME_REGEX.matcher(partitionField).matches()) {
                            throw new ConfigException(name, value, String.format("Invalid column name %s", partitionField));
                        }
                    }
                });

            } catch (Throwable e) {
                if (e instanceof ConfigException) {
                    throw e;
                } else {
                    throw new ConfigException(name, value, e.getMessage());
                }
            }

        }

        public static List<String> parsePartitionBy(String str) {
            return stringToList(str, COMMA_NO_PARENS_REGEX);
        }

        public static Pair<String, Integer> transformArgPair(String argsStr) {
            String[] parts = argsStr.split(",");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
            }
            return Pair.of(parts[0].trim(), Integer.parseInt(parts[1].trim()));
        }
    }

    public static class IdColumnsValidator implements ConfigDef.Validator {
        public static final IdColumnsValidator INSTANCE = new IdColumnsValidator();

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return;
            }
            try {
                String str = (String) value;
                List<String> idColumns = stringToList(str, COMMA_NO_PARENS_REGEX);
                idColumns.forEach(idColumn -> {
                    if (!COLUMN_NAME_REGEX.matcher(idColumn).matches()) {
                        throw new ConfigException(name, value, String.format("Invalid column name %s", idColumn));
                    }
                });
            } catch (Throwable e) {
                if (e instanceof ConfigException) {
                    throw e;
                } else {
                    throw new ConfigException(name, value, e.getMessage());
                }
            }
        }
    }

    public static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }
        Matcher matcher = LIST_STRING_REGEX.matcher(value);
        if (matcher.matches()) {
            value = matcher.group(1);
        } else {
            throw new ConfigException("", value, String.format("Invalid list string %s", value));
        }
        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }

    public enum Transform {
        YEAR, MONTH, DAY, HOUR, BUCKET, TRUNCATE;

        public static Transform fromString(String str) {
            switch (str.toLowerCase(Locale.ROOT)) {
                case "year":
                    return YEAR;
                case "month":
                    return MONTH;
                case "day":
                    return DAY;
                case "hour":
                    return HOUR;
                case "bucket":
                    return BUCKET;
                case "truncate":
                    return TRUNCATE;
                default:
                    throw new IllegalArgumentException("Invalid transform function " + str);
            }
        }
    }

}
