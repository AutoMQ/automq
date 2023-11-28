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

package com.automq.stream.s3.failover;

import com.automq.stream.utils.CommandResult;
import com.automq.stream.utils.CommandUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DefaultServerless implements Serverless {
    private static final String SERVERLESS_CMD = "/opt/automq/scripts/amq-serverless";

    @Override
    public String attach(String volumeId, int nodeId) throws ExecutionException {
        String[] cmd = new String[]{SERVERLESS_CMD, "volume", "attach", "-v", volumeId, "-n", Integer.toString(nodeId)};
        CommandResult result = CommandUtils.run(cmd);
        check(cmd, result);
        return jsonParse(result.stdout(), AttachResult.class).getDeviceName();
    }

    @Override
    public void delete(String volumeId) throws ExecutionException {
        String[] cmd = new String[]{SERVERLESS_CMD, "volume", "delete", "-v", volumeId};
        CommandResult result = CommandUtils.run(cmd);
        check(cmd, result);
    }

    @Override
    public void fence(String volumeId) throws ExecutionException {
        String[] cmd = new String[]{SERVERLESS_CMD, "volume", "fence", "-v", volumeId};
        CommandResult result = CommandUtils.run(cmd);
        check(cmd, result);
    }

    @Override
    public List<FailedNode> scan() throws ExecutionException {
        String[] cmd = new String[]{SERVERLESS_CMD, "volume", "queryFailover"};
        CommandResult result = CommandUtils.run(cmd);
        check(cmd, result);
        QueryFailedNode[] nodes = jsonParse(result.stdout(), QueryFailedNode[].class);
        return Arrays.stream(nodes).map(n -> {
            FailedNode failedNode = new FailedNode();
            failedNode.setNodeId(Integer.parseInt(n.getFirstBindNodeId()));
            failedNode.setVolumeId(n.getVolumeId());
            return failedNode;
        }).collect(Collectors.toList());
    }

    private static void check(String[] cmd, CommandResult rst) throws ExecutionException {
        if (rst.code() != 0) {
            throw new ExecutionException("Run " + Arrays.toString(cmd) + ", code:" + rst.code() + "  failed: " + rst.stderr(), null);
        }
    }

    private static <T> T jsonParse(String raw, Class<T> clazz) throws ExecutionException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(raw, clazz);
        } catch (JsonProcessingException e) {
            throw new ExecutionException("json parse (" + raw + ") fail", e);
        }
    }

    static class AttachResult {
        private String deviceName;

        public String getDeviceName() {
            return deviceName;
        }

        public void setDeviceName(String deviceName) {
            this.deviceName = deviceName;
        }
    }

    static class QueryFailedNode {
        private String firstBindNodeId;
        private String volumeId;

        public String getFirstBindNodeId() {
            return firstBindNodeId;
        }

        public void setFirstBindNodeId(String firstBindNodeId) {
            this.firstBindNodeId = firstBindNodeId;
        }

        public String getVolumeId() {
            return volumeId;
        }

        public void setVolumeId(String volumeId) {
            this.volumeId = volumeId;
        }
    }
}
