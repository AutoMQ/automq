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

import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DeleteRecordsResponse extends AbstractResponse {

    public static final long INVALID_LOW_WATERMARK = -1L;
    private final DeleteRecordsResponseData data;

    /**
     * Possible error code:
     *
     * OFFSET_OUT_OF_RANGE (1)
     * UNKNOWN_TOPIC_OR_PARTITION (3)
     * NOT_LEADER_FOR_PARTITION (6)
     * REQUEST_TIMED_OUT (7)
     * UNKNOWN (-1)
     */

    public DeleteRecordsResponse(DeleteRecordsResponseData data) {
        this.data = data;
    }

    public DeleteRecordsResponse(Struct struct, short version) {
        this.data = new DeleteRecordsResponseData(struct, version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public DeleteRecordsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (DeleteRecordsResponseData.DeleteRecordsTopicResult topicResponses : data.topics()) {
            for (DeleteRecordsResponseData.DeleteRecordsPartitionResult response : topicResponses.partitions()) {
                updateErrorCounts(errorCounts, Errors.forCode(response.errorCode()));
            }
        }
        return errorCounts;
    }

    public static DeleteRecordsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsResponse(ApiKeys.DELETE_RECORDS.parseResponse(version, buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
