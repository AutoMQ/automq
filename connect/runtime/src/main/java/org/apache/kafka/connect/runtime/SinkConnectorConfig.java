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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.transforms.util.RegexValidator;

import java.util.Map;

/**
 * Configuration needed for all sink connectors
 */

public class SinkConnectorConfig extends ConnectorConfig {

    public static final String TOPICS_CONFIG = SinkTask.TOPICS_CONFIG;
    private static final String TOPICS_DOC = "List of topics to consume, separated by commas";
    public static final String TOPICS_DEFAULT = "";
    private static final String TOPICS_DISPLAY = "Topics";

    public static final String TOPICS_REGEX_CONFIG = SinkTask.TOPICS_REGEX_CONFIG;
    private static final String TOPICS_REGEX_DOC = "Regular expression giving topics to consume. " +
        "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
        "Only one of " + TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG + " should be specified.";
    public static final String TOPICS_REGEX_DEFAULT = "";
    private static final String TOPICS_REGEX_DISPLAY = "Topics regex";

    static ConfigDef config = ConnectorConfig.configDef()
        .define(TOPICS_CONFIG, ConfigDef.Type.LIST, TOPICS_DEFAULT, ConfigDef.Importance.HIGH, TOPICS_DOC, COMMON_GROUP, 4, ConfigDef.Width.LONG, TOPICS_DISPLAY)
        .define(TOPICS_REGEX_CONFIG, ConfigDef.Type.STRING, TOPICS_REGEX_DEFAULT, new RegexValidator(), ConfigDef.Importance.HIGH, TOPICS_REGEX_DOC, COMMON_GROUP, 4, ConfigDef.Width.LONG, TOPICS_REGEX_DISPLAY);

    public static ConfigDef configDef() {
        return config;
    }

    public SinkConnectorConfig(Plugins plugins, Map<String, String> props) {
        super(plugins, config, props);
    }

    /**
     * Throw an exception if the passed-in properties do not constitute a valid sink.
     * @param props sink configuration properties
     */
    public static void validate(Map<String, String> props) {
        final boolean hasTopicsConfig = hasTopicsConfig(props);
        final boolean hasTopicsRegexConfig = hasTopicsRegexConfig(props);

        if (hasTopicsConfig && hasTopicsRegexConfig) {
            throw new ConfigException(SinkTask.TOPICS_CONFIG + " and " + SinkTask.TOPICS_REGEX_CONFIG +
                " are mutually exclusive options, but both are set.");
        }

        if (!hasTopicsConfig && !hasTopicsRegexConfig) {
            throw new ConfigException("Must configure one of " +
                SinkTask.TOPICS_CONFIG + " or " + SinkTask.TOPICS_REGEX_CONFIG);
        }
    }

    public static boolean hasTopicsConfig(Map<String, String> props) {
        String topicsStr = props.get(TOPICS_CONFIG);
        return topicsStr != null && !topicsStr.trim().isEmpty();
    }

    public static boolean hasTopicsRegexConfig(Map<String, String> props) {
        String topicsRegexStr = props.get(TOPICS_REGEX_CONFIG);
        return topicsRegexStr != null && !topicsRegexStr.trim().isEmpty();
    }

}
