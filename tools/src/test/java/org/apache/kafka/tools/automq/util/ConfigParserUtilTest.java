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
package org.apache.kafka.tools.automq.util;

import org.apache.kafka.tools.automq.model.ServerGroupConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigParserUtilTest {

    @Test
    void generatedServerConfig() {
        ServerGroupConfig config = ConfigParserUtil.genControllerConfig("192.168.0.1:9093;192.168.0.2:9094;192.168.0.3:9095", false);
        assertEquals(3, config.getNodeIdList().size());
        assertEquals("0@192.168.0.1:9093,1@192.168.0.2:9094,2@192.168.0.3:9095", config.getQuorumVoters());
        List<String> listenerList = Arrays.asList("PLAINTEXT://192.168.0.1:9092,CONTROLLER://192.168.0.1:9093", "PLAINTEXT://192.168.0.2:9092,CONTROLLER://192.168.0.2:9094", "PLAINTEXT://192.168.0.3:9092,CONTROLLER://192.168.0.3:9095");
        for (int i = 0; i < config.getListenerMap().size(); i++) {
            assertEquals(listenerList.get(i), config.getListenerMap().get(i));
        }

    }

    @Test
    void genBrokerConfig() {
        ServerGroupConfig controllerGroupConfig = ConfigParserUtil.genControllerConfig("192.168.0.1:9093;192.168.0.2:9094;192.168.0.3:9095", false);
        List<String> controllerListeners = Arrays.asList("PLAINTEXT://192.168.0.1:9092,CONTROLLER://192.168.0.1:9093", "PLAINTEXT://192.168.0.2:9092,CONTROLLER://192.168.0.2:9094", "PLAINTEXT://192.168.0.3:9092,CONTROLLER://192.168.0.3:9095");
        for (int i = 0; i < controllerGroupConfig.getListenerMap().size(); i++) {
            int nodeId = controllerGroupConfig.getNodeIdList().get(i);
            assertEquals(controllerListeners.get(i), controllerGroupConfig.getListenerMap().get(nodeId));
        }

        ServerGroupConfig brokerGroupConfig = ConfigParserUtil.genBrokerConfig("192.168.0.4:9092;192.168.0.5:9092;192.168.0.6:9092", controllerGroupConfig);

        Assertions.assertEquals(3, brokerGroupConfig.getNodeIdList().get(0));
        Assertions.assertEquals(4, brokerGroupConfig.getNodeIdList().get(1));
        Assertions.assertEquals(5, brokerGroupConfig.getNodeIdList().get(2));
        List<String> listenerList = Arrays.asList("PLAINTEXT://192.168.0.4:9092", "PLAINTEXT://192.168.0.5:9092", "PLAINTEXT://192.168.0.6:9092");
        for (int i = 0; i < brokerGroupConfig.getListenerMap().size(); i++) {
            int nodeId = brokerGroupConfig.getNodeIdList().get(i);
            assertEquals(listenerList.get(i), brokerGroupConfig.getListenerMap().get(nodeId));
        }

    }
}