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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigParserUtil {

    public static ServerGroupConfig genControllerConfig(String ipPortList, boolean isControllerOnlyMode) {
        String[] ipPortPairs = ipPortList.split(";");
        List<Integer> nodeIdList = new ArrayList<>();
        StringBuilder quorumVoters = new StringBuilder();
        Map<Integer, String> listenerMap = new HashMap<>();
        Map<Integer, String> advertisedListenerMap = new HashMap<>();

        for (int i = 0; i < ipPortPairs.length; i++) {
            nodeIdList.add(i);
            // Build quorumVoters
            if (i > 0) {
                quorumVoters.append(",");
            }
            quorumVoters.append(i).append("@").append(ipPortPairs[i]);

            if (isControllerOnlyMode) {
                listenerMap.put(i, "CONTROLLER://" + ipPortPairs[i]);
            } else {
                if ("9092".equals(ipPortPairs[i].split(":")[1])) {
                    throw new UnsupportedOperationException("Controller port can not be 9092 in server mode,because it will conflict with broker port");
                }

                // server force to listen 9092 by default
                String advertisedListener = "PLAINTEXT://" + ipPortPairs[i].split(":")[0] + ":9092";
                listenerMap.put(i, advertisedListener + "," + "CONTROLLER://" + ipPortPairs[i]);
                advertisedListenerMap.put(i, advertisedListener);
            }
        }

        return new ServerGroupConfig(nodeIdList, quorumVoters.toString(), listenerMap, advertisedListenerMap);
    }

    public static ServerGroupConfig genBrokerConfig(String ipPortList,
        ServerGroupConfig controllerGroupConfig) {
        String[] ipPortPairs = ipPortList.split(";");
        List<Integer> nodeIdList = new ArrayList<>();
        Map<Integer, String> listenerMap = new HashMap<>();
        int startIndex = controllerGroupConfig.getNodeIdList().size();
        for (int i = startIndex; i < startIndex + ipPortPairs.length; i++) {
            listenerMap.put(i, "PLAINTEXT://" + ipPortPairs[i - startIndex]);
            nodeIdList.add(i);
        }

        return new ServerGroupConfig(
            nodeIdList,
            controllerGroupConfig.getQuorumVoters(),
            listenerMap, listenerMap);

    }
}
