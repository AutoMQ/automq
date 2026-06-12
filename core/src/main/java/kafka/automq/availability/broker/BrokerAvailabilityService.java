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

package kafka.automq.availability.broker;

import kafka.automq.availability.AvailabilityRuntimeHooks;
import kafka.automq.availability.BrokerAvailabilityMonitor;
import kafka.automq.availability.action.AvailabilityActionExecutor;
import kafka.automq.availability.transport.AvailabilityKvRequestSender;

import com.automq.stream.s3.cache.blockcache.ColdReadInflightRegistry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;

/**
 * Broker-owned lifecycle holder for availability publishing and action polling components.
 */
public class BrokerAvailabilityService {
    private static final Logger LOGGER = new LogContext("[AVAILABILITY_FALLBACK] ").logger(BrokerAvailabilityService.class);

    private final AvailabilitySignalPublisher signalPublisher;
    private final BrokerActionReceiver actionReceiver;
    private final ActionResponsePublisher responsePublisher;
    private final BrokerAvailabilityMonitor monitor;
    private final Supplier<List<RecoveryAction>> retainedActionSupplier;
    private final AvailabilityActionExecutor actionExecutor;
    private final AvailabilityKvRequestSender sender;
    private final ScheduledExecutorService scheduler;
    private final Time time;
    private final long signalPublishIntervalMs;
    private final long actionPollIntervalMs;
    private final Map<Uuid, ActionResponse> openRecoverActionResponses = new ConcurrentHashMap<>();
    private final Set<Uuid> openRecoverActionsInflight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> signalTask;
    private volatile ScheduledFuture<?> actionTask;

    public BrokerAvailabilityService(BrokerAvailabilityMonitor monitor, AvailabilitySignalPublisher signalPublisher,
                                     BrokerActionReceiver actionReceiver, ActionResponsePublisher responsePublisher,
                                     Supplier<List<RecoveryAction>> retainedActionSupplier,
                                     AvailabilityActionExecutor actionExecutor,
                                     AvailabilityKvRequestSender sender,
                                     ScheduledExecutorService scheduler,
                                     Time time,
                                     long signalPublishIntervalMs,
                                     long actionPollIntervalMs) {
        this.monitor = monitor;
        this.signalPublisher = signalPublisher;
        this.actionReceiver = actionReceiver;
        this.responsePublisher = responsePublisher;
        this.retainedActionSupplier = retainedActionSupplier;
        this.actionExecutor = actionExecutor;
        this.sender = sender;
        this.scheduler = scheduler;
        this.time = time;
        this.signalPublishIntervalMs = signalPublishIntervalMs;
        this.actionPollIntervalMs = actionPollIntervalMs;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        if (monitor != null) {
            AvailabilityRuntimeHooks.register(monitor);
        }
        AvailabilityRuntimeHooks.registerOpenRecoverActionConsumer(this::consumeOpenRecoverAction);
        if (scheduler != null && signalPublisher != null && signalPublishIntervalMs > 0L) {
            signalTask = scheduler.scheduleWithFixedDelay(this::publishSignalsIfRunning, 0L,
                signalPublishIntervalMs, TimeUnit.MILLISECONDS);
        }
        if (scheduler != null && actionReceiver != null && actionExecutor != null && responsePublisher != null
            && actionPollIntervalMs > 0L) {
            actionTask = scheduler.scheduleWithFixedDelay(this::executeActionsIfRunning, actionPollIntervalMs,
                actionPollIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        cancel(signalTask);
        cancel(actionTask);
        if (monitor != null) {
            AvailabilityRuntimeHooks.unregister(monitor);
        }
        ColdReadInflightRegistry.clear();
        AvailabilityRuntimeHooks.unregisterOpenRecoverActionConsumer(this::consumeOpenRecoverAction);
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    private void publishSignalsIfRunning() {
        if (running.get()) {
            signalPublisher.publishOnceAsync().whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    LOGGER.warn("Failed to publish availability signals", throwable);
                }
            });
        }
    }

    private void executeActionsIfRunning() {
        if (!running.get()) {
            return;
        }
        long nowMs = time.milliseconds();
        for (RecoveryAction action : actionReceiver.pollActions(retainedActionSupplier.get(), nowMs)) {
            if (!actionExecutor.supports(action.getActionType())) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Skip unsupported availability action actionUuid={} type={} target={}",
                        action.getActionUuid(), action.getActionType(), action.getTarget());
                }
                continue;
            }
            LOGGER.info("Execute availability action actionUuid={} type={} target={} dryRun={} deadlineMs={}",
                action.getActionUuid(), action.getActionType(), action.getTarget(), action.isDryRun(),
                action.getDeadlineMs());
            ActionResponse response = actionExecutor.execute(action);
            if (response == null) {
                LOGGER.warn("Availability action returned null response actionUuid={} type={} target={}",
                    action.getActionUuid(), action.getActionType(), action.getTarget());
                continue;
            }
            LOGGER.info("Availability action completed actionUuid={} type={} target={} status={} failureReason={}",
                response.getActionUuid(), response.getActionType(), response.getTarget(), response.getStatus(),
                response.getFailureReason());
            publishAndDelete(action, response);
        }
    }

    private boolean consumeOpenRecoverAction(AvailabilityActionType actionType,
                                             org.apache.kafka.common.TopicPartition topicPartition,
                                             AvailabilityRuntimeHooks.OpenRecoverActionOperation operation) {
        if (!running.get() || retainedActionSupplier == null || responsePublisher == null) {
            return false;
        }
        long nowMs = time.milliseconds();
        for (RecoveryAction action : actionReceiver.pollActions(retainedActionSupplier.get(), nowMs)) {
            if (action.getActionType() != actionType || !targetMatches(action.getTarget(), topicPartition)) {
                continue;
            }
            if (openRecoverActionResponses.containsKey(action.getActionUuid()) ||
                !openRecoverActionsInflight.add(action.getActionUuid())) {
                return false;
            }
            if (action.isDryRun()) {
                LOGGER.info("Dry-run open recover availability action actionUuid={} type={} target={}",
                    action.getActionUuid(), actionType, action.getTarget());
                completeOpenRecoverAction(action, new ActionResponse(action.getActionUuid(), actionType, action.getTarget(),
                    ActionExecutionStatus.DRY_RUN, nowMs, nowMs, null));
                return false;
            }
            try {
                LOGGER.info("Execute open recover availability action actionUuid={} type={} target={}",
                    action.getActionUuid(), actionType, action.getTarget());
                operation.run(action);
                completeOpenRecoverAction(action, new ActionResponse(action.getActionUuid(), actionType, action.getTarget(),
                    ActionExecutionStatus.SUCCEEDED, nowMs, nowMs, null));
                LOGGER.info("Open recover availability action completed actionUuid={} type={} target={} status={}",
                    action.getActionUuid(), actionType, action.getTarget(), ActionExecutionStatus.SUCCEEDED);
                return true;
            } catch (Exception e) {
                completeOpenRecoverAction(action, new ActionResponse(action.getActionUuid(), actionType, action.getTarget(),
                    ActionExecutionStatus.FAILED, nowMs, nowMs, e.getClass().getName() + ": " + e.getMessage()));
                LOGGER.warn("Open recover availability action failed actionUuid={} type={} target={}",
                    action.getActionUuid(), actionType, action.getTarget(), e);
                return false;
            } finally {
                openRecoverActionsInflight.remove(action.getActionUuid());
            }
        }
        return false;
    }

    private void completeOpenRecoverAction(RecoveryAction action, ActionResponse response) {
        openRecoverActionResponses.put(action.getActionUuid(), response);
        publishAndDelete(action, response);
    }

    private boolean targetMatches(AvailabilityTarget target, TopicPartition topicPartition) {
        return target.getKind() == AvailabilityTarget.Kind.TOPIC_PARTITION &&
            topicPartition.topic().equals(target.getTopic()) &&
            topicPartition.partition() == target.getPartition();
    }

    private void publishAndDelete(RecoveryAction action, ActionResponse response) {
        responsePublisher.publishAsync(response).thenCompose(ignored -> {
            if (sender == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Skip availability action delete because sender is unavailable actionUuid={} type={} target={}",
                        action.getActionUuid(), action.getActionType(), action.getTarget());
                }
                return CompletableFuture.completedFuture(null);
            }
            return sender.deleteBrokerAction(action.getActionUuid().toString());
        }).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                LOGGER.warn("Failed to publish response or delete availability action actionUuid={} type={} target={}",
                    action.getActionUuid(), action.getActionType(), action.getTarget(), throwable);
            }
        });
    }

    private void cancel(ScheduledFuture<?> task) {
        if (task != null) {
            task.cancel(false);
        }
    }
}
