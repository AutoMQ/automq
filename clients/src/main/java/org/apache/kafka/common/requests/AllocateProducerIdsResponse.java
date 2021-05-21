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

import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class AllocateProducerIdsResponse extends AbstractResponse {

    private final AllocateProducerIdsResponseData data;

    public AllocateProducerIdsResponse(AllocateProducerIdsResponseData data) {
        super(ApiKeys.ALLOCATE_PRODUCER_IDS);
        this.data = data;
    }

    @Override
    public AllocateProducerIdsResponseData data() {
        return data;
    }

    /**
     * The number of each type of error in the response, including {@link Errors#NONE} and top-level errors as well as
     * more specifically scoped errors (such as topic or partition-level errors).
     *
     * @return A count of errors.
     */
    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public static AllocateProducerIdsResponse parse(ByteBuffer buffer, short version) {
        return new AllocateProducerIdsResponse(new AllocateProducerIdsResponseData(
                new ByteBufferAccessor(buffer), version));
    }
}
