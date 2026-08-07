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
package org.apache.kafka.common.requests.s3;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AutomqPreparePartitionHandoffRequestData;
import org.apache.kafka.common.message.AutomqPreparePartitionHandoffResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies the public request and response contract for partition metadata handoff preparation. */
@Tag("S3Unit")
public class AutomqPreparePartitionHandoffRequestTest {

    /**
     * Given multiple handoffs and records, the broker protocol preserves the complete whole-request payload.
     */
    @Test
    public void testRequestRoundTripPreservesWholeBatch() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        AutomqPreparePartitionHandoffRequestData data = new AutomqPreparePartitionHandoffRequestData()
            .setHandoffs(List.of(
                handoff(topicId, 3, 42L, 7L, new byte[] {1, 2, 3}),
                handoff(topicId, 4, 51L, 9L, new byte[] {4, 5})));
        AutomqPreparePartitionHandoffRequest request = new AutomqPreparePartitionHandoffRequest.Builder(data).build();

        RequestHeader header = new RequestHeader(request.apiKey(), request.version(), "test", 1);
        RequestContext context = new RequestContext(
            header,
            "connection",
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS,
            new ListenerName("BROKER"),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false);
        ByteBuffer serialized = request.serializeWithHeader(header);
        serialized.position(header.size());
        AbstractRequest parsed = context.parseRequest(serialized.slice()).request;
        AutomqPreparePartitionHandoffRequestData parsedData =
            ((AutomqPreparePartitionHandoffRequest) parsed).data();

        assertEquals(2, parsedData.handoffs().size());
        assertEquals(topicId, parsedData.handoffs().get(0).topicId());
        assertEquals(3, parsedData.handoffs().get(0).partitionIndex());
        assertEquals(42L, parsedData.handoffs().get(0).metaStreamHandoffEndOffset());
        assertEquals(7L, parsedData.handoffs().get(0).records().get(0).baseOffset());
        assertArrayEquals(new byte[] {1, 2, 3}, parsedData.handoffs().get(0).records().get(0).metaKeyValue());
    }

    /**
     * Given a request failure, the protocol returns one top-level result and no per-handoff results.
     */
    @Test
    public void testResponseHasWholeRequestOutcome() {
        AutomqPreparePartitionHandoffRequest request = new AutomqPreparePartitionHandoffRequest.Builder(
            new AutomqPreparePartitionHandoffRequestData()).build();

        AbstractResponse response = request.getErrorResponse(12, Errors.INVALID_REQUEST.exception());
        AutomqPreparePartitionHandoffResponseData data =
            ((AutomqPreparePartitionHandoffResponse) response).data();

        assertEquals(Errors.INVALID_REQUEST.code(), data.errorCode());
        assertEquals(12, data.throttleTimeMs());
    }

    private static AutomqPreparePartitionHandoffRequestData.Handoff handoff(
        Uuid topicId,
        int partitionIndex,
        long handoffEndOffset,
        long baseOffset,
        byte[] value
    ) {
        return new AutomqPreparePartitionHandoffRequestData.Handoff()
            .setTopicId(topicId)
            .setPartitionIndex(partitionIndex)
            .setMetaStreamHandoffEndOffset(handoffEndOffset)
            .setRecords(List.of(new AutomqPreparePartitionHandoffRequestData.HandoffRecord()
                .setBaseOffset(baseOffset)
                .setMetaKeyValue(value)));
    }
}
