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

package kafka.automq.availability.controller;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.controller.availability.RecoveryDecision;
import org.apache.kafka.controller.availability.RecoveryDecisionType;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;

/**
 * Controller-owned lifecycle holder for the availability reconcile loop.
 */
public class ControllerAvailabilityService implements Reconfigurable {
    private static final Logger LOGGER = new LogContext("[AVAILABILITY_FALLBACK] ").logger(ControllerAvailabilityService.class);

    private final ControllerAvailabilityReconciler reconciler;
    private final AutoFallbackRuntimeConfig runtimeConfig;
    private final ScheduledExecutorService executor;
    private final long reconcileIntervalMs;
    private final BooleanSupplier activeControllerSupplier;
    private final Runnable shutdownHook;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> reconcileTask;

    public ControllerAvailabilityService(ControllerAvailabilityReconciler reconciler,
                                         ScheduledExecutorService executor,
                                         long reconcileIntervalMs) {
        this(reconciler, null, executor, reconcileIntervalMs, () -> true, () -> { });
    }

    public ControllerAvailabilityService(ControllerAvailabilityReconciler reconciler,
                                         ScheduledExecutorService executor,
                                         long reconcileIntervalMs,
                                         Runnable shutdownHook) {
        this(reconciler, null, executor, reconcileIntervalMs, () -> true, shutdownHook);
    }

    public ControllerAvailabilityService(ControllerAvailabilityReconciler reconciler,
                                         ScheduledExecutorService executor,
                                         long reconcileIntervalMs,
                                         BooleanSupplier activeControllerSupplier,
                                         Runnable shutdownHook) {
        this(reconciler, null, executor, reconcileIntervalMs, activeControllerSupplier, shutdownHook);
    }

    public ControllerAvailabilityService(ControllerAvailabilityReconciler reconciler,
                                         AutoFallbackRuntimeConfig runtimeConfig,
                                         ScheduledExecutorService executor,
                                         long reconcileIntervalMs,
                                         BooleanSupplier activeControllerSupplier,
                                         Runnable shutdownHook) {
        this.reconciler = reconciler;
        this.runtimeConfig = runtimeConfig;
        this.executor = executor;
        this.reconcileIntervalMs = reconcileIntervalMs;
        this.activeControllerSupplier = activeControllerSupplier;
        this.shutdownHook = shutdownHook;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        reconcileTask = executor.scheduleWithFixedDelay(this::reconcileIfRunning, 0L, reconcileIntervalMs,
            TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        running.set(false);
        ScheduledFuture<?> task = reconcileTask;
        if (task != null) {
            task.cancel(false);
        }
        shutdownHook.run();
        executor.shutdown();
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return AutoFallbackRuntimeConfig.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        if (runtimeConfig != null) {
            runtimeConfig.validateReconfiguration(configs);
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        if (runtimeConfig != null) {
            runtimeConfig.reconfigure(configs);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    public void runOnceForTest() {
        reconcileIfRunning();
    }

    private void reconcileIfRunning() {
        if (running.get() && activeControllerSupplier.getAsBoolean()) {
            logReconcileResult(reconciler.reconcileOnce());
        }
    }

    private void logReconcileResult(ControllerAvailabilityReconciler.ReconcileResult result) {
        RecoveryDecision decision = result.decision();
        if (result.consumedResponses() > 0) {
            LOGGER.info("Consumed availability action responses count={}", result.consumedResponses());
        }
        if (decision.type() == RecoveryDecisionType.NOOP) {
            return;
        }
        if (decision.type() == RecoveryDecisionType.BLOCKED) {
            LOGGER.info("Availability fallback decision type={} attributionType={} attributionTarget={} sourceSignal={} attributionReason={} blockedReason={} nextEligibleMs={}",
                decision.type(), result.attribution().type(), result.attribution().target(),
                result.attribution().sourceSignalType(), result.attribution().reason(), decision.blockedReason(),
                decision.nextEligibleMs());
            return;
        }
        RecoveryAction action = decision.action();
        LOGGER.info("Availability fallback decision type={} attributionType={} attributionTarget={} sourceSignal={} actionUuid={} actionType={} actionTarget={} executorBrokerId={} dryRun={} deadlineMs={} reason={}",
            decision.type(), result.attribution().type(), result.attribution().target(),
            result.attribution().sourceSignalType(), action.getActionUuid(), action.getActionType(),
            action.getTarget(), action.getExecutorBrokerId(), action.isDryRun(), action.getDeadlineMs(),
            action.getReason());
    }
}
