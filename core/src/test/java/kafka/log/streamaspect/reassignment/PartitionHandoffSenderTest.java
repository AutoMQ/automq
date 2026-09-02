/*
 * Copyright 2026, AutoMQ HK Limited.
 *
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
package kafka.log.streamaspect.reassignment;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AutomqPreparePartitionHandoffResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.AutomqPreparePartitionHandoffRequest;
import org.apache.kafka.common.requests.s3.AutomqPreparePartitionHandoffResponse;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies partition handoff batching, timeout, and one-attempt sender behavior. */
@Tag("S3Unit")
public class PartitionHandoffSenderTest {
    private static final Executor DIRECT_EXECUTOR = Runnable::run;
    private static final long TEST_TIMEOUT_MS = 10_000;

    /**
     * Given an exactly valid encoded body and an oversized handoff, only the valid handoff is sent.
     */
    @Test
    public void testExactSizeBoundaryAndOversizedFallback() {
        PartitionHandoff handoff = handoff(0, 32);
        int exactSize = PartitionHandoffSender.encodedRequestBodySize(List.of(handoff));
        CapturingNetwork network = new CapturingNetwork();
        PartitionHandoffSender exactSender = new PartitionHandoffSender(
            network, exactSize, TEST_TIMEOUT_MS, DIRECT_EXECUTOR);

        CompletableFuture<Void> exact = exactSender.send(node(1), handoff);
        assertEquals(1, network.requests.size());
        network.succeed(0);
        exact.join();

        CapturingNetwork oversizedNetwork = new CapturingNetwork();
        PartitionHandoffSender oversizedSender = new PartitionHandoffSender(
            oversizedNetwork, exactSize - 1, TEST_TIMEOUT_MS, DIRECT_EXECUTOR);
        CompletableFuture<Void> oversized = oversizedSender.send(node(1), handoff);

        assertFailureReason(oversized, PartitionHandoffSendException.Reason.HANDOFF_TOO_LARGE);
        assertEquals(0, oversizedNetwork.requests.size());
    }

    /**
     * Given one request is inflight, handoffs queued behind it are sent together in the next size-bounded batch.
     */
    @Test
    public void testInflightRequestBatchesQueuedHandoffs() {
        CapturingNetwork network = new CapturingNetwork();
        PartitionHandoff first = handoff(0, 16);
        PartitionHandoff second = handoff(1, 16);
        PartitionHandoff third = handoff(2, 16);
        int maximumSize = PartitionHandoffSender.encodedRequestBodySize(List.of(second, third));
        PartitionHandoffSender sender = new PartitionHandoffSender(
            network, maximumSize, TEST_TIMEOUT_MS, DIRECT_EXECUTOR);

        CompletableFuture<Void> firstResult = sender.send(node(1), first);
        CompletableFuture<Void> secondResult = sender.send(node(1), second);
        CompletableFuture<Void> thirdResult = sender.send(node(1), third);
        assertEquals(1, network.requests.size());

        network.succeed(0);

        assertEquals(2, network.requests.size());
        assertEquals(2, network.requests.get(1).data().handoffs().size());
        network.succeed(1);
        firstResult.join();
        secondResult.join();
        thirdResult.join();
    }

    /**
     * Given enough handoffs to grow the compact-array prefix, batching still uses the exact encoded size without a count cap.
     */
    @Test
    public void testEncodedSizeAcrossCompactArrayBoundary() {
        CapturingNetwork network = new CapturingNetwork();
        PartitionHandoff blocker = handoff(-1, 1);
        List<PartitionHandoff> handoffs = new ArrayList<>();
        for (int partitionId = 0; partitionId < 128; partitionId++) {
            handoffs.add(handoff(partitionId, 1));
        }
        int exactSize = PartitionHandoffSender.encodedRequestBodySize(handoffs);
        PartitionHandoffSender sender = new PartitionHandoffSender(
            network, exactSize, TEST_TIMEOUT_MS, DIRECT_EXECUTOR);
        CompletableFuture<Void> blockerResult = sender.send(node(1), blocker);
        List<CompletableFuture<Void>> results = handoffs.stream()
            .map(handoff -> sender.send(node(1), handoff))
            .toList();

        assertEquals(1, network.requests.size());
        network.succeed(0);

        blockerResult.join();
        assertEquals(2, network.requests.size());
        assertEquals(128, network.requests.get(1).data().handoffs().size());
        network.succeed(1);
        results.forEach(CompletableFuture::join);
    }

    /**
     * Given size-split batches for one target, the next request waits for the prior whole-request outcome.
     */
    @Test
    public void testSizeSplitKeepsOneInflightPerTarget() {
        CapturingNetwork network = new CapturingNetwork();
        PartitionHandoff first = handoff(0, 16);
        PartitionHandoff second = handoff(1, 16);
        int oneHandoffSize = PartitionHandoffSender.encodedRequestBodySize(List.of(first));
        PartitionHandoffSender sender = new PartitionHandoffSender(
            network, oneHandoffSize, TEST_TIMEOUT_MS, DIRECT_EXECUTOR);

        CompletableFuture<Void> firstResult = sender.send(node(1), first);
        CompletableFuture<Void> secondResult = sender.send(node(1), second);
        assertEquals(1, network.requests.size());

        network.succeed(0);

        firstResult.join();
        assertEquals(2, network.requests.size());
        network.succeed(1);
        secondResult.join();
    }

    /**
     * Given one target is awaiting a response, a different target still sends independently.
     */
    @Test
    public void testDifferentTargetsProgressIndependently() {
        CapturingNetwork network = new CapturingNetwork();
        PartitionHandoff handoff = handoff(0, 16);
        int exactSize = PartitionHandoffSender.encodedRequestBodySize(List.of(handoff));
        PartitionHandoffSender sender = new PartitionHandoffSender(
            network, exactSize, TEST_TIMEOUT_MS, DIRECT_EXECUTOR);

        sender.send(node(1), handoff);
        sender.send(node(2), handoff(1, 16));

        assertEquals(List.of(1, 2), network.targetIds());
    }

    /**
     * Given a whole-request error, every handoff in that request falls back together.
     */
    @Test
    public void testWholeRequestFailureCompletesEntireBatch() {
        CapturingNetwork network = new CapturingNetwork();
        PartitionHandoffSender sender = new PartitionHandoffSender(
            network, 1024, TEST_TIMEOUT_MS, DIRECT_EXECUTOR);

        CompletableFuture<Void> blocker = sender.send(node(1), handoff(-1, 16));
        CompletableFuture<Void> first = sender.send(node(1), handoff(0, 16));
        CompletableFuture<Void> second = sender.send(node(1), handoff(1, 16));
        network.succeed(0);
        blocker.join();
        network.fail(1, Errors.BROKER_NOT_AVAILABLE);

        assertFailureReason(first, PartitionHandoffSendException.Reason.SEND_FAILURE);
        assertFailureReason(second, PartitionHandoffSendException.Reason.SEND_FAILURE);
    }

    /** Given the target response is lost, the RPC timeout falls back without an application retry. */
    @Test
    public void testResponseLossTimesOutWithoutRetry() {
        CapturingNetwork network = new CapturingNetwork();
        PartitionHandoff handoff = handoff(0, 16);
        PartitionHandoffSender sender = new PartitionHandoffSender(
            network, PartitionHandoffSender.encodedRequestBodySize(List.of(handoff)), 10, DIRECT_EXECUTOR);

        CompletableFuture<Void> result = sender.send(node(1), handoff);

        assertFailureReason(result, PartitionHandoffSendException.Reason.SEND_TIMEOUT);
        assertEquals(1, network.requests.size());
    }

    private static void assertFailureReason(
        CompletableFuture<Void> future,
        PartitionHandoffSendException.Reason reason
    ) {
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        PartitionHandoffSendException failure = assertInstanceOf(
            PartitionHandoffSendException.class, exception.getCause());
        assertEquals(reason, failure.reason());
    }

    private static Node node(int id) {
        return new Node(id, "localhost", 9000 + id);
    }

    private static PartitionHandoff handoff(int partitionId, int valueSize) {
        return new PartitionHandoff(
            Uuid.fromString("FbrrdcfRQbqRKTp9h7B1YQ"),
            partitionId,
            new MetaStreamHandoff(100 + partitionId, List.of(
                new MetaStreamHandoffRecord(3, ByteBuffer.wrap(new byte[valueSize])))));
    }

    private static final class CapturingNetwork implements PartitionHandoffSender.RequestSender {
        private final List<Node> targets = new ArrayList<>();
        private final List<AutomqPreparePartitionHandoffRequest> requests = new ArrayList<>();
        private final List<CompletableFuture<AutomqPreparePartitionHandoffResponse>> responses = new ArrayList<>();

        @Override
        public CompletableFuture<AutomqPreparePartitionHandoffResponse> send(
            Node target,
            AutomqPreparePartitionHandoffRequest.Builder builder
        ) {
            targets.add(target);
            requests.add(builder.build());
            CompletableFuture<AutomqPreparePartitionHandoffResponse> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        }

        private void succeed(int index) {
            complete(index, Errors.NONE);
        }

        private void fail(int index, Errors error) {
            complete(index, error);
        }

        private void complete(int index, Errors error) {
            responses.get(index).complete(new AutomqPreparePartitionHandoffResponse(
                new AutomqPreparePartitionHandoffResponseData().setErrorCode(error.code())));
        }

        private List<Integer> targetIds() {
            return targets.stream().map(Node::id).toList();
        }
    }

}
