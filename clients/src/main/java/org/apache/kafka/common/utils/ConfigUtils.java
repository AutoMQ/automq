/*
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

package org.apache.kafka.common.utils;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigUtils {

    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * Translates deprecated configurations into their non-deprecated equivalents
     *
     * This is a convenience method for {@link ConfigUtils#translateDeprecatedConfigs(Map, Map)}
     * until we can use Java 9+ {@code Map.of(..)} and {@code Set.of(...)}
     *
     * @param configs the input configuration
     * @param aliasGroups An array of arrays of synonyms.  Each synonym array begins with the non-deprecated synonym
     *                    For example, new String[][] { { a, b }, { c, d, e} }
     *                    would declare b as a deprecated synonym for a,
     *                    and d and e as deprecated synonyms for c.
     *                    The ordering of synonyms determines the order of precedence
     *                    (e.g. the first synonym takes precedence over the second one)
     * @return a new configuration map with deprecated  keys translated to their non-deprecated equivalents
     */
    public static <T> Map<String, T> translateDeprecatedConfigs(Map<String, T> configs, String[][] aliasGroups) {
        return translateDeprecatedConfigs(configs, Stream.of(aliasGroups)
            .collect(Collectors.toMap(x -> x[0], x -> Stream.of(x).skip(1).collect(Collectors.toList()))));
    }

    /**
     * Translates deprecated configurations into their non-deprecated equivalents
     *
     * @param configs the input configuration
     * @param aliasGroups A map of config to synonyms.  Each key is the non-deprecated synonym
     *                    For example, Map.of(a , Set.of(b), c, Set.of(d, e))
     *                    would declare b as a deprecated synonym for a,
     *                    and d and e as deprecated synonyms for c.
     *                    The ordering of synonyms determines the order of precedence
     *                    (e.g. the first synonym takes precedence over the second one)
     * @return a new configuration map with deprecated  keys translated to their non-deprecated equivalents
     */
    public static <T> Map<String, T> translateDeprecatedConfigs(Map<String, T> configs,
                                                                Map<String, List<String>> aliasGroups) {
        Set<String> aliasSet = Stream.concat(
            aliasGroups.keySet().stream(),
            aliasGroups.values().stream().flatMap(Collection::stream))
            .collect(Collectors.toSet());

        // pass through all configurations without aliases
        Map<String, T> newConfigs = configs.entrySet().stream()
            .filter(e -> !aliasSet.contains(e.getKey()))
            // filter out null values
            .filter(e -> Objects.nonNull(e.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        aliasGroups.forEach((target, aliases) -> {
            List<String> deprecated = aliases.stream()
                .filter(configs::containsKey)
                .collect(Collectors.toList());

            if (deprecated.isEmpty()) {
                // No deprecated key(s) found.
                if (configs.containsKey(target)) {
                    newConfigs.put(target, configs.get(target));
                }
                return;
            }

            String aliasString = String.join(", ", deprecated);

            if (configs.containsKey(target)) {
                // Ignore the deprecated key(s) because the actual key was set.
                log.error(target + " was configured, as well as the deprecated alias(es) " +
                          aliasString + ".  Using the value of " + target);
                newConfigs.put(target, configs.get(target));
            } else if (deprecated.size() > 1) {
                log.error("The configuration keys " + aliasString + " are deprecated and may be " +
                          "removed in the future.  Additionally, this configuration is ambiguous because " +
                          "these configuration keys are all aliases for " + target + ".  Please update " +
                          "your configuration to have only " + target + " set.");
                newConfigs.put(target, configs.get(deprecated.get(0)));
            } else {
                log.warn("Configuration key " + deprecated.get(0) + " is deprecated and may be removed " +
                         "in the future.  Please update your configuration to use " + target + " instead.");
                newConfigs.put(target, configs.get(deprecated.get(0)));
            }
        });

        return newConfigs;
    }

    public static String configMapToRedactedString(Map<String, Object> map, ConfigDef configDef) {
        StringBuilder bld = new StringBuilder("{");
        List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        String prefix = "";
        for (String key : keys) {
            bld.append(prefix).append(key).append("=");
            ConfigKey configKey = configDef.configKeys().get(key);
            if (configKey == null || configKey.type().isSensitive()) {
                bld.append("(redacted)");
            } else {
                Object value = map.get(key);
                if (value == null) {
                    bld.append("null");
                } else if (configKey.type() == Type.STRING) {
                    bld.append("\"").append(value).append("\"");
                } else {
                    bld.append(value);
                }
            }
            prefix = ", ";
        }
        bld.append("}");
        return bld.toString();
    }

    /**
     * Finds and returns a boolean configuration option from the configuration map or the default value if the option is
     * not set.
     *
     * @param configs Map with the configuration options
     * @param key Configuration option for which the boolean value will be returned
     * @param defaultValue The default value that will be used when the key is not present
     * @return A boolean value of the configuration option of the default value
     */
    public static boolean getBoolean(final Map<String, Object> configs, final String key, final boolean defaultValue) {
        final Object value = configs.getOrDefault(key, defaultValue);
        if (value instanceof Boolean) {
            return (boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else {
            log.error("Invalid value (" + value + ") on configuration '" + key + "'. The default value '" + defaultValue + "' will be used instead. Please specify a true/false value.");
            return defaultValue;
        }
    }

    // AutoMQ injection start
    /**
     * Finds and returns a boolean configuration option from the configuration map.
     *
     * @param configs Map with the configuration options
     * @param key Configuration option for which the boolean value will be returned
     * @return A boolean value of the configuration option
     * @throws NoSuchElementException if the configuration option is not set
     * @throws IllegalArgumentException if the configuration option is not a boolean or a string
     */
    public static boolean getBoolean(final Map<String, Object> configs, final String key) {
        final Object value = configs.get(key);
        if (value == null) {
            throw new NoSuchElementException("Missing required configuration '" + key + "'");
        }
        if (value instanceof Boolean) {
            return (boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new IllegalArgumentException("Invalid value (" + value + ") on configuration '" + key + "'. Please specify a true/false value.");
    }

    /**
     * Finds and returns an integer configuration option from the configuration map
     *
     * @param configs Map with the configuration options
     * @param key Configuration option for which the integer value will be returned
     * @return An integer value of the configuration option
     * @throws NumberFormatException if the configuration option is not a valid integer
     * @throws NoSuchElementException if the configuration option is not set
     * @throws IllegalArgumentException if the configuration option is not a integer or a string
     */
    public static int getInteger(final Map<String, Object> configs, final String key) {
        final Object value = configs.get(key);
        if (value == null) {
            throw new NoSuchElementException("Missing required configuration '" + key + "'");
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new IllegalArgumentException("Invalid value (" + value + ") on configuration '" + key + "'. Please specify an integer value.");
    }

    /**
     * Finds and returns a long configuration option from the configuration map
     *
     * @param configs Map with the configuration options
     * @param key Configuration option for which the long value will be returned
     * @return A long value of the configuration option
     * @throws NumberFormatException if the configuration option is not a valid long
     * @throws NoSuchElementException if the configuration option is not set
     * @throws IllegalArgumentException if the configuration option is not a long or a string
     */
    public static long getLong(final Map<String, Object> configs, final String key) {
        final Object value = configs.get(key);
        if (value == null) {
            throw new NoSuchElementException("Missing required configuration '" + key + "'");
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Invalid value (" + value + ") on configuration '" + key + "'. Please specify a long value.");
    }

    /**
     * Finds and returns a double configuration option from the configuration map
     *
     * @param configs Map with the configuration options
     * @param key Configuration option for which the double value will be returned
     * @return A double value of the configuration option
     * @throws NumberFormatException if the configuration option is not a valid double
     * @throws NoSuchElementException if the configuration option is not set
     * @throws IllegalArgumentException if the configuration option is not a double or a string
     */
    public static double getDouble(final Map<String, Object> configs, final String key) {
        final Object value = configs.get(key);
        if (value == null) {
            throw new NoSuchElementException("Missing required configuration '" + key + "'");
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new IllegalArgumentException("Invalid value (" + value + ") on configuration '" + key + "'. Please specify a double value.");
    }

    public static double getDouble(final Map<String, Object> configs, final String key, final double defaultValue) {
        final Object value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new IllegalArgumentException("Invalid value (" + value + ") on configuration '" + key + "'. Please specify a double value.");
    }
    // AutoMQ injection end
}
