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

import kafka.automq.utils.AsyncSender;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.AutomqPreparePartitionHandoffRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.s3.AutomqPreparePartitionHandoffRequest;
import org.apache.kafka.common.requests.s3.AutomqPreparePartitionHandoffResponse;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.ThreadUtils;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.automq.stream.utils.LockUtils.runInLock;

/**
 * Sends optional partition handoff hints in bounded, per-target batches.
 * A failed, lost, timed-out, or oversized send always resolves to fallback and is never retried here.
 *
 * <p>This class is thread-safe. The supplied network sender remains caller-owned and is not closed by this class;
 * callers should discard the sender with its broker lifecycle.
 */
public final class PartitionHandoffSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionHandoffSender.class);
    public static final int DEFAULT_MAX_REQUEST_BODY_SIZE = 32 * 1024 * 1024;
    public static final int DEFAULT_TIMEOUT_MS = 100;
    private static final short API_VERSION = 0;
    private static final int REQUEST_TAGGED_FIELDS_SIZE = 1;
    private static final int SINGLE_HANDOFF_ENVELOPE_SIZE =
        REQUEST_TAGGED_FIELDS_SIZE + ByteUtils.sizeOfUnsignedVarint(2);
    private static final ExecutorService RESPONSE_EXECUTOR = Threads.newFixedThreadPool(
        1, ThreadUtils.createThreadFactory("partition-handoff-response-%d", true), LOGGER);

    private final RequestSender requestSender;
    private final int maximumRequestBodySize;
    private final long timeoutMs;
    private final Executor responseExecutor;
    private final Map<Node, TargetQueue> targets = new ConcurrentHashMap<>();

    /**
     * Creates a sender using the standard broker network client and ticket 09 defaults.
     *
     * @param asyncSender broker network client
     */
    public PartitionHandoffSender(AsyncSender asyncSender) {
        this(adapt(asyncSender), DEFAULT_MAX_REQUEST_BODY_SIZE, DEFAULT_TIMEOUT_MS, RESPONSE_EXECUTOR);
    }

    /**
     * Creates a sender with explicit request-size and timeout bounds.
     *
     * @param requestSender broker request boundary
     * @param maximumRequestBodySize maximum actual encoded request-body bytes
     * @param timeoutMs independent send timeout in milliseconds
     */
    public PartitionHandoffSender(
        RequestSender requestSender,
        int maximumRequestBodySize,
        long timeoutMs
    ) {
        this(requestSender, maximumRequestBodySize, timeoutMs, RESPONSE_EXECUTOR);
    }

    /**
     * Creates a sender with explicit bounds and response executor.
     *
     * @param requestSender broker request boundary
     * @param maximumRequestBodySize maximum actual encoded request-body bytes
     * @param timeoutMs independent send timeout in milliseconds
     * @param responseExecutor executor used to process RPC responses and complete handoff results
     */
    public PartitionHandoffSender(
        RequestSender requestSender,
        int maximumRequestBodySize,
        long timeoutMs,
        Executor responseExecutor
    ) {
        if (maximumRequestBodySize <= 0) {
            throw new IllegalArgumentException("maximumRequestBodySize must be positive");
        }
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive");
        }
        this.requestSender = requestSender;
        this.maximumRequestBodySize = maximumRequestBodySize;
        this.timeoutMs = timeoutMs;
        this.responseExecutor = Objects.requireNonNull(responseExecutor, "responseExecutor");
    }

    /**
     * Queues one handoff for a target and reports the stable send outcome. An unsuccessful result means the caller
     * must use normal metadata-stream recovery.
     *
     * @param target target broker
     * @param handoff frozen partition handoff
     * @return a future that completes after the target acknowledges the whole containing batch, or completes
     *         exceptionally when the request is rejected, times out, or cannot be sent
     */
    public CompletableFuture<Void> send(
        Node target,
        PartitionHandoff handoff
    ) {
        if (encodedRequestBodySize(List.of(handoff)) > maximumRequestBodySize) {
            return failed(PartitionHandoffSendException.Reason.HANDOFF_TOO_LARGE);
        }
        PendingHandoff pending = new PendingHandoff(handoff);
        targets.computeIfAbsent(target, TargetQueue::new).add(pending);
        return pending.result;
    }

    /**
     * Returns the exact version-0 encoded request-body size for the supplied handoffs.
     *
     * @param handoffs handoffs to encode
     * @return encoded body bytes, excluding Kafka request header and frame length
     */
    public static int encodedRequestBodySize(Collection<PartitionHandoff> handoffs) {
        return requestData(handoffs).size(new ObjectSerializationCache(), API_VERSION);
    }

    private static AutomqPreparePartitionHandoffRequestData requestData(
        Collection<PartitionHandoff> handoffs
    ) {
        return new AutomqPreparePartitionHandoffRequestData()
            .setHandoffs(handoffs.stream().map(PartitionHandoff::toProtocol).collect(Collectors.toList()));
    }

    private static RequestSender adapt(AsyncSender asyncSender) {
        return (target, builder) -> asyncSender.sendRequest(target, builder).thenApply(response -> {
            if (!response.hasResponse()) {
                throw new PartitionHandoffSendException(PartitionHandoffSendException.Reason.SEND_FAILURE);
            }
            return (AutomqPreparePartitionHandoffResponse) response.responseBody();
        });
    }

    private final class TargetQueue {
        private final Node target;
        private final Deque<PendingHandoff> queue = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock();
        private boolean inflight;

        private TargetQueue(Node target) {
            this.target = target;
        }

        private void add(PendingHandoff handoff) {
            runInLock(lock, () -> {
                queue.add(handoff);
                sendNext();
            });
        }

        private void sendNext() {
            if (inflight || queue.isEmpty()) {
                return;
            }
            Batch batch = new Batch();
            while (!queue.isEmpty() && batch.encodedSizeWith(queue.peek()) <= maximumRequestBodySize) {
                batch.add(queue.remove());
            }
            inflight = true;
            send(batch);
        }

        private void send(Batch batch) {
            AutomqPreparePartitionHandoffRequest.Builder builder =
                new AutomqPreparePartitionHandoffRequest.Builder(requestData(batch.values()));
            CompletableFuture<AutomqPreparePartitionHandoffResponse> response;
            try {
                response = requestSender.send(target, builder);
            } catch (RuntimeException exception) {
                complete(batch, new PartitionHandoffSendException(
                    PartitionHandoffSendException.Reason.SEND_FAILURE, exception));
                return;
            }
            response.orTimeout(timeoutMs, TimeUnit.MILLISECONDS).whenCompleteAsync((value, exception) -> {
                if (isTimeout(exception)) {
                    complete(batch, new PartitionHandoffSendException(
                        PartitionHandoffSendException.Reason.SEND_TIMEOUT, exception));
                } else if (exception != null || value.data().errorCode() != Errors.NONE.code()) {
                    complete(batch, new PartitionHandoffSendException(
                        PartitionHandoffSendException.Reason.SEND_FAILURE, exception));
                } else {
                    complete(batch, null);
                }
            }, responseExecutor);
        }

        private void complete(Batch batch, PartitionHandoffSendException exception) {
            runInLock(lock, () -> {
                inflight = false;
                batch.handoffs.forEach(handoff -> {
                    if (exception == null) {
                        handoff.result.complete(null);
                    } else {
                        handoff.result.completeExceptionally(exception);
                    }
                });
                sendNext();
            });
        }
    }

    private static final class Batch {
        private final List<PendingHandoff> handoffs = new ArrayList<>();
        private int handoffBytes;

        private Collection<PartitionHandoff> values() {
            return handoffs.stream().map(pending -> pending.handoff).collect(Collectors.toList());
        }

        private void add(PendingHandoff handoff) {
            handoffs.add(handoff);
            handoffBytes += encodedHandoffSize(handoff.handoff);
        }

        private int encodedSizeWith(PendingHandoff additional) {
            return encodedSize(handoffs.size() + 1,
                handoffBytes + encodedHandoffSize(additional.handoff));
        }

        private static int encodedSize(int handoffCount, int handoffBytes) {
            return REQUEST_TAGGED_FIELDS_SIZE
                + ByteUtils.sizeOfUnsignedVarint(handoffCount + 1)
                + handoffBytes;
        }

        private static int encodedHandoffSize(PartitionHandoff handoff) {
            return handoff.encodedSize() - SINGLE_HANDOFF_ENVELOPE_SIZE;
        }
    }

    private static final class PendingHandoff {
        private final PartitionHandoff handoff;
        private final CompletableFuture<Void> result = new CompletableFuture<>();

        private PendingHandoff(PartitionHandoff handoff) {
            this.handoff = handoff;
        }
    }

    /**
     * Broker RPC boundary used by the batcher; implementations perform one network attempt per call.
     */
    @FunctionalInterface
    public interface RequestSender {
        /**
         * Sends one handoff request to a target broker.
         *
         * @param target target broker
         * @param builder whole-batch request
         * @return non-null response future; exceptional completion or an error response means fallback
         */
        CompletableFuture<AutomqPreparePartitionHandoffResponse> send(
            Node target,
            AutomqPreparePartitionHandoffRequest.Builder builder
        );
    }

    private static CompletableFuture<Void> failed(PartitionHandoffSendException.Reason reason) {
        return CompletableFuture.failedFuture(new PartitionHandoffSendException(reason));
    }

    private static boolean isTimeout(Throwable exception) {
        return exception instanceof java.util.concurrent.TimeoutException
            || exception instanceof org.apache.kafka.common.errors.TimeoutException;
    }
}
