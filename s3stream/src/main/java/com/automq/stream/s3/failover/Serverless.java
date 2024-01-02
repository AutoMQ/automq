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

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface Serverless {

    /**
     * Attach volume to the target node.
     *
     * @param volumeId volume id
     * @param nodeId   target node id
     * @return attached device name
     */
    String attach(String volumeId, int nodeId) throws ExecutionException;

    /**
     * Delete the volume
     *
     * @param volumeId volume id
     */
    void delete(String volumeId) throws ExecutionException;

    /**
     * Fence the first attached node access to the volume
     *
     * @param volumeId volume id
     */
    void fence(String volumeId) throws ExecutionException;

    /**
     * Scan failed node
     *
     * @return {@link FailedNode} list
     */
    List<FailedNode> scan() throws ExecutionException;

    class FailedNode {
        private int nodeId;
        private String volumeId;

        public int getNodeId() {
            return nodeId;
        }

        public void setNodeId(int nodeId) {
            this.nodeId = nodeId;
        }

        public String getVolumeId() {
            return volumeId;
        }

        public void setVolumeId(String volumeId) {
            this.volumeId = volumeId;
        }

        @Override
        public String toString() {
            return "FailedNode{" +
                "nodeId=" + nodeId +
                ", volumeId='" + volumeId + '\'' +
                '}';
        }
    }

}
