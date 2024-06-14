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
package org.apache.kafka.server.config;

public class DelegationTokenManagerConfigs {
    /** ********* Delegation Token Configuration ****************/
    public static final String DELEGATION_TOKEN_SECRET_KEY_CONFIG = "delegation.token.secret.key";
    public static final String DELEGATION_TOKEN_SECRET_KEY_DOC = "Secret key to generate and verify delegation tokens. The same key must be configured across all the brokers. " +
            " If using Kafka with KRaft, the key must also be set across all controllers. " +
            " If the key is not set or set to empty string, brokers will disable the delegation token support.";

    @Deprecated
    public static final String DELEGATION_TOKEN_SECRET_KEY_ALIAS_CONFIG = "delegation.token.master.key";
    public static final String DELEGATION_TOKEN_SECRET_KEY_ALIAS_DOC = "DEPRECATED: An alias for " + DELEGATION_TOKEN_SECRET_KEY_CONFIG + ", which should be used instead of this config.";

    public static final String DELEGATION_TOKEN_MAX_LIFETIME_CONFIG = "delegation.token.max.lifetime.ms";
    public static final long DELEGATION_TOKEN_MAX_LIFE_TIME_MS_DEFAULT = 7 * 24 * 60 * 60 * 1000L;
    public static final String DELEGATION_TOKEN_MAX_LIFE_TIME_DOC = "The token has a maximum lifetime beyond which it cannot be renewed anymore. Default value 7 days.";

    public static final String DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG = "delegation.token.expiry.time.ms";
    public static final long DELEGATION_TOKEN_EXPIRY_TIME_MS_DEFAULT = 24 * 60 * 60 * 1000L;
    public static final String DELEGATION_TOKEN_EXPIRY_TIME_MS_DOC = "The token validity time in milliseconds before the token needs to be renewed. Default value 1 day.";

    public static final String DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG = "delegation.token.expiry.check.interval.ms";
    public static final long DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_DEFAULT = 60 * 60 * 1000L;
    public static final String DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_DOC = "Scan interval to remove expired delegation tokens.";
}
