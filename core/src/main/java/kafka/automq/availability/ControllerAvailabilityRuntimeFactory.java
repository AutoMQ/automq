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

package kafka.automq.availability;

import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;

import kafka.automq.availability.controller.AvailabilityKvTransport;
import kafka.automq.availability.controller.AvailabilityPartitionReassignmentAdapter;
import kafka.automq.availability.controller.AvailabilityKvCleanup;
import kafka.automq.availability.controller.AutoFallbackRuntimeConfig;
import kafka.automq.availability.controller.ControllerActionExecutor;
import kafka.automq.availability.controller.ControllerAvailabilityReconciler;
import kafka.automq.availability.controller.ControllerAvailabilityService;
import kafka.automq.availability.controller.ControllerLocalAvailabilityKvSender;
import kafka.automq.availability.controller.KvBackedAvailabilityActionDispatcher;
import kafka.automq.availability.controller.PartitionReassignmentTargetSelector;
import kafka.automq.availability.transport.AvailabilityKvRequestSender;
import kafka.autobalancer.executor.ControllerActionExecutorService;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityAttributionEngine;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.CoverageBroker;
import org.apache.kafka.controller.availability.ProtectionStateManager;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.controller.availability.RecoveryActionPlanner;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.LeaderConstants;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds Controller-owned availability reconciliation and action dispatch runtime.
 */
public final class ControllerAvailabilityRuntimeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerAvailabilityRuntimeFactory.class);

    private ControllerAvailabilityRuntimeFactory() {
    }

    public static ControllerAvailabilityService controllerService(KafkaConfig config,
                                                                  Time time,
                                                                  Controller controller,
                                                                  MetadataImageReader imageReader) {
        AvailabilityKvTransport transport = new AvailabilityKvTransport(reader ->
            imageReader.read(image -> {
                reader.accept(image.kv());
                return null;
            }));
        AvailabilityKvRequestSender sender = new ControllerLocalAvailabilityKvSender(controller);
        ControllerActionExecutorService actionExecutorService = new ControllerActionExecutorService(controller);
        actionExecutorService.start();
        KvControllerActionStateStore stateStore = new KvControllerActionStateStore(sender);
        ControllerActionExecutor controllerActionExecutor = new ControllerActionExecutor(
            Map.of(AvailabilityActionType.PARTITION_REASSIGNMENT,
                partitionReassignmentAdapter(actionExecutorService, imageReader)::execute),
            stateStore,
            time);
        AvailabilityKvCleanup kvCleanup = new AvailabilityKvCleanup(
            (namespace, key) -> sender.delete(AvailabilityKvTransport.deleteRequest(namespace, key)).join(),
            (namespace, key, exception) -> LOGGER.warn("Availability KV cleanup failed key={}/{} reason={}",
                namespace, key, exception.getMessage()),
            config.automqAutoFallbackResponseRetentionMs(),
            config.automqAutoFallbackActionCleanupGraceMs());
        AutoFallbackRuntimeConfig runtimeConfig = new AutoFallbackRuntimeConfig(config);
        ControllerAvailabilityReconciler reconciler = new ControllerAvailabilityReconciler(
            signalSource(transport),
            () -> imageReader.read(ControllerAvailabilityRuntimeFactory::coverageBrokers),
            new AvailabilityAttributionEngine(config.automqAutoFallbackGlobalSharedStorageAffectedRatio(),
                config.automqAutoFallbackGlobalSharedStorageSmallClusterBrokers()),
            new ProtectionStateManager(),
            new RecoveryActionPlanner(runtimeConfig::plannerConfig),
            new KvBackedAvailabilityActionDispatcher(sender, controllerActionExecutor,
                EnumSet.of(AvailabilityActionType.PARTITION_REASSIGNMENT),
                action -> imageReader.read(image -> brokerActionOwner(image, action))),
            (action, response) -> sender.put(AvailabilityKvTransport.putActionResponseRequest(response)),
            controllerActionExecutor::restore,
            nowMs -> kvCleanup.cleanup(nowMs, transport.collectBrokerActions(), transport.collectActionResponses(),
                transport.collectControllerActionStates()),
            time,
            config.automqAutoFallbackSignalStaleMs());
        ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("availability-controller-runtime-%d", true), LOGGER);
        return new ControllerAvailabilityService(reconciler, runtimeConfig, scheduler,
            config.automqAutoFallbackControllerReconcileIntervalMs(), controller::isActive,
            actionExecutorService::shutdown);
    }

    public interface MetadataImageReader {
        <T> T read(Function<MetadataImage, T> reader);
    }

    static ControllerAvailabilityReconciler.SignalSource signalSource(AvailabilityKvTransport transport) {
        return new ControllerAvailabilityReconciler.SignalSource() {
            @Override
            public List<BrokerAvailabilitySnapshot> collectSignals() {
                return transport.collectSignals();
            }

            @Override
            public List<ActionResponse> collectActionResponses() {
                return transport.collectActionResponses();
            }

            @Override
            public List<ControllerActionState> collectControllerActionStates() {
                return transport.collectControllerActionStates();
            }

            @Override
            public List<RecoveryAction> collectBrokerActions() {
                return transport.collectBrokerActions();
            }
        };
    }

    private static AvailabilityPartitionReassignmentAdapter partitionReassignmentAdapter(ControllerActionExecutorService actionExecutorService,
                                                                                        MetadataImageReader imageReader) {
        return new AvailabilityPartitionReassignmentAdapter(actionExecutorService,
            new PartitionReassignmentTargetSelector(),
            () -> imageReader.read(ControllerAvailabilityRuntimeFactory::candidateBrokers),
            topicPartition -> imageReader.read(image -> currentOwner(image, topicPartition)));
    }

    static List<PartitionReassignmentTargetSelector.CandidateBroker> candidateBrokers(MetadataImage image) {
        return image.cluster().brokers().entrySet().stream()
            .map(entry -> new PartitionReassignmentTargetSelector.CandidateBroker(entry.getKey(),
                !entry.getValue().fenced(), true))
            .collect(Collectors.toList());
    }

    private static int currentOwner(MetadataImage image, TopicPartition topicPartition) {
        if (image.topics().getTopic(topicPartition.topic()) == null) {
            throw new IllegalStateException("topic not found " + topicPartition.topic());
        }
        PartitionRegistration registration = image.topics().getTopic(topicPartition.topic())
            .partitions().get(topicPartition.partition());
        if (registration == null || registration.leader == LeaderConstants.NO_LEADER) {
            throw new IllegalStateException("partition leader not found " + topicPartition);
        }
        return registration.leader;
    }

    private static int brokerActionOwner(MetadataImage image, RecoveryAction action) {
        AvailabilityTarget target = action.getTarget();
        if (target.getKind() == AvailabilityTarget.Kind.BROKER) {
            return target.getBrokerId();
        }
        return currentOwner(image, new TopicPartition(target.getTopic(), target.getPartition()));
    }

    private static List<CoverageBroker> coverageBrokers(MetadataImage image) {
        return image.cluster().brokers().entrySet().stream()
            .filter(entry -> !entry.getValue().fenced())
            .map(entry -> new CoverageBroker(entry.getKey(), entry.getValue().epoch()))
            .collect(Collectors.toList());
    }

    private static final class KvControllerActionStateStore implements ControllerActionExecutor.ControllerActionStateStore {
        private final AvailabilityKvRequestSender sender;
        private final Map<Uuid, ControllerActionState> states = new ConcurrentHashMap<>();

        private KvControllerActionStateStore(AvailabilityKvRequestSender sender) {
            this.sender = sender;
        }

        @Override
        public ControllerActionState get(Uuid actionUuid) {
            return states.get(actionUuid);
        }

        @Override
        public void put(ControllerActionState state) {
            states.put(state.getAction().getActionUuid(), state);
            sender.put(AvailabilityKvTransport.putControllerActionStateRequest(state));
        }
    }
}
