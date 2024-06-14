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

public class ZooKeeperInternals {
    /**
     * This string is used in ZooKeeper in several places to indicate a default entity type.
     * For example, default user quotas are stored under /config/users/&ltdefault&gt
     * Note that AdminClient does <b>not</b> use this to indicate a default, nor do records in KRaft mode.
     * This constant will go away in Apache Kafka 4.0 with the end of ZK mode.
     */
    public static final String DEFAULT_STRING = "<default>";
}
