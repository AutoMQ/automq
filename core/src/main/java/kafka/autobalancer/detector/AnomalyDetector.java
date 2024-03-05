/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.detector;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AnomalyDetector {
    private final Logger logger;
    private final List<Goal> goalsByPriority;
    private final ClusterModel clusterModel;
    private final ScheduledExecutorService executorService;
    private final ActionExecutorService actionExecutor;
    private final Set<Integer> excludedBrokers;
    private final Set<String> excludedTopics;
    private final int maxActionsNumPerExecution;
    private final long detectInterval;
    private final long maxTolerateMetricsDelayMs;
    private final long coolDownIntervalPerActionMs;
    private volatile boolean running;

    AnomalyDetector(LogContext logContext, int maxActionsNumPerDetect, long detectIntervalMs, long maxTolerateMetricsDelayMs,
                    long coolDownIntervalPerActionMs, ClusterModel clusterModel, ActionExecutorService actionExecutor,
                    List<Goal> goals, Set<Integer> excludedBrokers, Set<String> excludedTopics) {
        this.logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
        this.maxActionsNumPerExecution = maxActionsNumPerDetect;
        this.detectInterval = detectIntervalMs;
        this.maxTolerateMetricsDelayMs = maxTolerateMetricsDelayMs;
        this.coolDownIntervalPerActionMs = coolDownIntervalPerActionMs;
        this.clusterModel = clusterModel;
        this.actionExecutor = actionExecutor;
        this.executorService = Executors.newSingleThreadScheduledExecutor(new AutoBalancerThreadFactory("anomaly-detector"));
        this.goalsByPriority = goals;
        Collections.sort(this.goalsByPriority);
        this.excludedBrokers = excludedBrokers;
        this.excludedTopics = excludedTopics;
        this.running = false;
        logger.info("maxActionsNumPerDetect: {}, detectInterval: {}ms, coolDownIntervalPerAction: {}ms, goals: {}, excluded brokers: {}, excluded topics: {}",
                this.maxActionsNumPerExecution, this.detectInterval, coolDownIntervalPerActionMs, this.goalsByPriority, this.excludedBrokers, this.excludedTopics);
    }

    public void start() {
        this.actionExecutor.start();
        this.executorService.schedule(this::detect, detectInterval, TimeUnit.MILLISECONDS);
        logger.info("Started");
    }

    public void shutdown() throws InterruptedException {
        this.running = false;
        this.executorService.shutdown();
        try {
            if (!this.executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                this.executorService.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }

        this.actionExecutor.shutdown();
        logger.info("Shutdown completed");
    }

    public void pause() {
        this.running = false;
    }

    public void resume() {
        this.running = true;
    }

    public void detect() {
        long nextExecutionDelay = detectInterval;
        try {
            nextExecutionDelay = detect0();
        } catch (Exception e) {
            logger.error("Detect error", e);
        }
        logger.info("Detect finished, next detect will be after {} ms", nextExecutionDelay);
        this.executorService.schedule(this::detect, nextExecutionDelay, TimeUnit.MILLISECONDS);
    }

    long detect0() {
        if (!this.running) {
            logger.info("Not active controller, skip detect");
            return detectInterval;
        }
        logger.info("Start detect");
        // The delay in processing kraft log could result in outdated cluster snapshot
        ClusterModelSnapshot snapshot = this.clusterModel.snapshot(excludedBrokers, excludedTopics, this.maxTolerateMetricsDelayMs);

        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            logger.info("Broker status: {}", broker.shortString());
            if (logger.isDebugEnabled()) {
                for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica : snapshot.replicasFor(broker.getBrokerId())) {
                    logger.debug("Replica status {}", replica.shortString());
                }
            }
        }

        List<Action> totalActions = new ArrayList<>();
        for (Goal goal : goalsByPriority) {
            if (!this.running) {
                break;
            }
            totalActions.addAll(goal.optimize(snapshot, goalsByPriority));
        }
        int totalActionSize = totalActions.size();
        List<Action> actionsToExecute = checkAndMergeActions(totalActions);
        logger.info("Total actions num: {}, executable num: {}", totalActionSize, actionsToExecute.size());
        this.actionExecutor.execute(actionsToExecute);

        return actionsToExecute.size() * this.coolDownIntervalPerActionMs + this.detectInterval;
    }

    List<Action> checkAndMergeActions(List<Action> actions) throws IllegalStateException {
        actions = actions.subList(0, Math.min(actions.size(), maxActionsNumPerExecution));
        List<Action> filteredActions = new ArrayList<>();
        Map<TopicPartition, Action> actionMergeMap = new HashMap<>();
        for (Action action : actions) {
            if (action.getType() == ActionType.SWAP) {
                Action prevAction = actionMergeMap.remove(action.getSrcTopicPartition());
                if (prevAction != null && prevAction.getDestBrokerId() != action.getSrcBrokerId()) {
                    throw new IllegalStateException(String.format("Unmatched action chains for %s, prev: %s, next: %s",
                            action.getSrcTopicPartition(), prevAction, action));
                }
                prevAction = actionMergeMap.remove(action.getDestTopicPartition());
                if (prevAction != null && prevAction.getDestBrokerId() != action.getDestBrokerId()) {
                    throw new IllegalStateException(String.format("Unmatched action chains for %s, prev: %s, next: %s",
                            action.getSrcTopicPartition(), prevAction, action));
                }
                filteredActions.add(action);
                continue;
            }
            Action prevAction = actionMergeMap.get(action.getSrcTopicPartition());
            if (prevAction == null) {
                filteredActions.add(action);
                actionMergeMap.put(action.getSrcTopicPartition(), action);
                continue;
            }
            if (prevAction.getDestBrokerId() != action.getSrcBrokerId()) {
                throw new IllegalStateException(String.format("Unmatched action chains for %s, prev: %s, next: %s",
                        action.getSrcTopicPartition(), prevAction, action));
            }
            prevAction.setDestBrokerId(action.getDestBrokerId());
        }

        return filteredActions;
    }
}
