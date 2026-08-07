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
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerRegistration;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Option;

/**
 * Owns all process-local fast partition reassignment resources for one broker lifecycle.
 *
 * <p>The manager is the only broker runtime boundary for sending, receiving, and destructively consuming handoff
 * hints. It also owns the sender's network resources and discards all ephemeral target state on close.
 * Handoffs remain optional performance hints; every unsuccessful send or take requires metadata-stream fallback by
 * {@link kafka.log.streamaspect.MetaStream}.
 */
public abstract class FastPartitionReassignmentManager implements AutoCloseable {
    private static final FastPartitionReassignmentManager DISABLED =
        new Disabled();
    private static volatile FastPartitionReassignmentManager instance = DISABLED;

    /** Returns the shared inert manager used by MetaStreams outside a broker handoff lifecycle. */
    public static FastPartitionReassignmentManager disabled() {
        return DISABLED;
    }

    /**
     * Initializes the production singleton and all broker-lifecycle handoff resources.
     *
     * @param config broker configuration used for networking and target resolution
     * @param metrics broker metrics registry used by the network sender
     * @param metadataCache current cluster metadata used to resolve the target broker
     * @throws IllegalStateException if the singleton is already initialized
     */
    public static synchronized void initialize(KafkaConfig config, Metrics metrics, MetadataCache metadataCache) {
        if (instance != DISABLED) {
            throw new IllegalStateException("FastPartitionReassignmentManager is already initialized");
        }
        AsyncSender networkSender = new AsyncSender.BrokersAsyncSender(
            config, metrics, "partition_handoff", Time.SYSTEM,
            "AUTOMQ_PARTITION_HANDOFF", new LogContext(), PartitionHandoffSender.DEFAULT_TIMEOUT_MS);
        PartitionHandoffSender handoffSender = new PartitionHandoffSender(networkSender);
        instance = create(
            handoffSender::send, new PartitionHandoffCache(),
            (topicId, partitionId) -> resolveTarget(
                metadataCache, config.interBrokerListenerName(), config.nodeId(), topicId, partitionId),
            networkSender::close);
    }

    /** Returns the production singleton, or the inert manager before initialization and after shutdown. */
    public static FastPartitionReassignmentManager instance() {
        return instance;
    }

    /** Detaches and closes the production singleton once for the current broker lifecycle. */
    public static synchronized void shutdown() {
        FastPartitionReassignmentManager current = instance;
        instance = DISABLED;
        current.close();
    }

    /**
     * Derives the current healthy reassignment target and sends one immutable handoff.
     *
     * @param handoff frozen MetaStream state to send when an eligible target exists
     * @return a future that completes after the target acknowledges the handoff; disabled operation, missing target,
     *         transport failure, timeout, and oversized handoff complete exceptionally with a stable reason
     */
    public abstract CompletableFuture<Void> send(PartitionHandoff handoff);

    /**
     * Receives a decoded request into the lifecycle-owned cache.
     *
     * @param handoffs complete request contents
     */
    public abstract void receive(Collection<PartitionHandoff> handoffs);

    /**
     * Destructively consumes the exact target handoff.
     *
     * @param key topic, partition, and Controller-authorized MetaStream end offset
     * @return consumed handoff, or empty when metadata-stream replay is required
     */
    public abstract Optional<PartitionHandoff> take(PartitionHandoff.Key key);

    /** Clears staged hints and closes lifecycle-owned sender resources once. */
    @Override
    public abstract void close();

    static FastPartitionReassignmentManager create(
        SendOperation sendOperation,
        PartitionHandoffCache cache,
        TargetResolver targetResolver,
        Runnable cleanup
    ) {
        return new Enabled(sendOperation, cache, targetResolver, cleanup);
    }

    static Node resolveTarget(
        MetadataCache metadataCache,
        ListenerName listenerName,
        int brokerId,
        Uuid topicId,
        int partitionId
    ) {
        if (!metadataCache.autoMQVersion().isFastPartitionReassignmentSupported()) {
            return null;
        }
        String topicName = metadataCache.topicIdsToNames().get(topicId);
        if (topicName == null) {
            return null;
        }
        Option<UpdateMetadataPartitionState> partition = metadataCache.getPartitionInfo(topicName, partitionId);
        if (partition.isEmpty() || partition.get().replicas().size() != 1) {
            return null;
        }
        int targetBrokerId = partition.get().replicas().get(0);
        if (targetBrokerId == brokerId) {
            return null;
        }
        BrokerRegistration registration = metadataCache.getNode(targetBrokerId);
        if (registration == null || registration.fenced() || registration.inControlledShutdown()) {
            return null;
        }
        Option<Node> target = metadataCache.getAliveBrokerNode(targetBrokerId, listenerName);
        return target.isDefined() ? target.get() : null;
    }

    @FunctionalInterface
    interface TargetResolver {
        Node resolve(Uuid topicId, int partitionId);
    }

    @FunctionalInterface
    interface SendOperation {
        CompletableFuture<Void> send(Node target, PartitionHandoff handoff);
    }

    private static final class Disabled extends FastPartitionReassignmentManager {
        @Override
        public CompletableFuture<Void> send(PartitionHandoff handoff) {
            return failed(PartitionHandoffSendException.Reason.NOT_ATTEMPTED);
        }

        @Override
        public void receive(Collection<PartitionHandoff> handoffs) {
        }

        @Override
        public Optional<PartitionHandoff> take(PartitionHandoff.Key key) {
            return Optional.empty();
        }

        @Override
        public void close() {
        }
    }

    private static final class Enabled extends FastPartitionReassignmentManager {
        private final SendOperation sendOperation;
        private final PartitionHandoffCache cache;
        private final TargetResolver targetResolver;
        private final Runnable cleanup;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Enabled(
            SendOperation sendOperation,
            PartitionHandoffCache cache,
            TargetResolver targetResolver,
            Runnable cleanup
        ) {
            this.sendOperation = sendOperation;
            this.cache = cache;
            this.targetResolver = targetResolver;
            this.cleanup = cleanup;
        }

        @Override
        public CompletableFuture<Void> send(PartitionHandoff handoff) {
            if (closed.get()) {
                return failed(PartitionHandoffSendException.Reason.NOT_ATTEMPTED);
            }
            Node target;
            try {
                target = targetResolver.resolve(handoff.key().topicId(), handoff.key().partitionId());
            } catch (RuntimeException exception) {
                return failed(PartitionHandoffSendException.Reason.SEND_FAILURE, exception);
            }
            if (target == null) {
                return failed(PartitionHandoffSendException.Reason.NOT_ATTEMPTED);
            }
            try {
                return sendOperation.send(target, handoff).handle((nil, exception) -> {
                    if (exception == null) {
                        return null;
                    }
                    throw PartitionHandoffSendException.from(exception);
                });
            } catch (RuntimeException exception) {
                return failed(PartitionHandoffSendException.Reason.SEND_FAILURE, exception);
            }
        }

        @Override
        public void receive(Collection<PartitionHandoff> handoffs) {
            if (!closed.get()) {
                cache.putAll(handoffs);
            }
        }

        @Override
        public Optional<PartitionHandoff> take(PartitionHandoff.Key key) {
            return closed.get() ? Optional.empty() : cache.take(key);
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            cache.clear();
            cleanup.run();
        }
    }

    private static CompletableFuture<Void> failed(PartitionHandoffSendException.Reason reason) {
        return CompletableFuture.failedFuture(new PartitionHandoffSendException(reason));
    }

    private static CompletableFuture<Void> failed(
        PartitionHandoffSendException.Reason reason,
        Throwable cause
    ) {
        return CompletableFuture.failedFuture(new PartitionHandoffSendException(reason, cause));
    }

}
