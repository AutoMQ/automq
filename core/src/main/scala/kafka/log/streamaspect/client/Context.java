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

package kafka.log.streamaspect.client;

import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;

public class Context {
    // TODO: remove sync test mode, test should keep the same logic as production code.
    private static boolean testMode = false;
    public KafkaConfig config;
    public BrokerServer brokerServer;

    public static void enableTestMode() {
        testMode = true;
    }

    public static boolean isTestMode() {
        return testMode;
    }
}
