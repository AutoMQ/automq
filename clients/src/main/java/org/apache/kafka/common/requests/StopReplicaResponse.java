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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StopReplicaResponse extends AbstractResponse {

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     * STALE_BROKER_EPOCH (77)
     */
    private final StopReplicaResponseData data;

    public StopReplicaResponse(StopReplicaResponseData data) {
        this.data = data;
    }

    public StopReplicaResponse(Struct struct, short version) {
        data = new StopReplicaResponseData(struct, version);
    }

    public List<StopReplicaPartitionError> partitionErrors() {
        return data.partitionErrors();
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        if (data.errorCode() != Errors.NONE.code())
            // Minor optimization since the top-level error applies to all partitions
            return Collections.singletonMap(error(), data.partitionErrors().size());
        return errorCounts(data.partitionErrors().stream().map(p -> Errors.forCode(p.errorCode())).collect(Collectors.toList()));
    }

    public static StopReplicaResponse parse(ByteBuffer buffer, short version) {
        return new StopReplicaResponse(ApiKeys.STOP_REPLICA.parseResponse(version, buffer), version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

}
