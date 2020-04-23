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

import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


public class CreatePartitionsResponse extends AbstractResponse {

    private final CreatePartitionsResponseData data;

    public CreatePartitionsResponse(CreatePartitionsResponseData data) {
        this.data = data;
    }

    public CreatePartitionsResponse(Struct struct, short version) {
        this.data = new CreatePartitionsResponseData(struct, version);
    }

    public CreatePartitionsResponseData data() {
        return data;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        data.results().forEach(result ->
            updateErrorCounts(counts, Errors.forCode(result.errorCode()))
        );
        return counts;
    }

    public static CreatePartitionsResponse parse(ByteBuffer buffer, short version) {
        return new CreatePartitionsResponse(ApiKeys.CREATE_PARTITIONS.parseResponse(version, buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }
}
