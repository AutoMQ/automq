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
import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.goals.GoalUtils;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import kafka.autobalancer.services.AbstractResumableService;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class AnomalyDetector extends AbstractResumableService {
    private static final double MAX_PARTITION_REASSIGNMENT_RATIO = 0.5;
    private final ClusterModel clusterModel;
    private final ScheduledExecutorService executorService;
    private final ActionExecutorService actionExecutor;
    private final Lock configChangeLock;
    private volatile List<Goal> goalsByPriority;
    private volatile Set<Integer> excludedBrokers;
    private volatile Set<String> excludedTopics;
    private volatile long detectInterval;
    private volatile long maxTolerateMetricsDelayMs;
    private volatile int executionConcurrency;
    private volatile long executionIntervalMs;
    private volatile boolean isLeader = false;
    private volatile Map<Integer, Boolean> slowBrokers = new HashMap<>();

    AnomalyDetector(LogContext logContext, long detectIntervalMs, long maxTolerateMetricsDelayMs, int executionConcurrency,
                    long executionIntervalMs, ClusterModel clusterModel, ActionExecutorService actionExecutor,
                    List<Goal> goals, Set<Integer> excludedBrokers, Set<String> excludedTopics) {
        super(logContext);
        this.configChangeLock = new ReentrantLock();
        this.detectInterval = detectIntervalMs;
        this.maxTolerateMetricsDelayMs = maxTolerateMetricsDelayMs;
        this.executionConcurrency = executionConcurrency;
        this.executionIntervalMs = executionIntervalMs;
        this.clusterModel = clusterModel;
        this.actionExecutor = actionExecutor;
        this.executorService = Executors.newSingleThreadScheduledExecutor(new AutoBalancerThreadFactory("anomaly-detector"));
        this.goalsByPriority = goals;
        Collections.sort(this.goalsByPriority);
        this.excludedBrokers = excludedBrokers;
        this.excludedTopics = excludedTopics;
        this.executorService.schedule(this::detect, detectInterval, TimeUnit.MILLISECONDS);
        S3StreamKafkaMetricsManager.setSlowBrokerSupplier(() -> this.slowBrokers);
        logger.info("detectInterval: {}ms, executionConcurrency: {}, executionIntervalMs: {}ms, goals: {}, excluded brokers: {}, excluded topics: {}",
                this.detectInterval, this.executionConcurrency, this.executionIntervalMs, this.goalsByPriority, this.excludedBrokers, this.excludedTopics);
    }

    @Override
    public void doRun() {

    }

    @Override
    public void doShutdown() {
        this.executorService.shutdown();
        try {
            if (!this.executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                this.executorService.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void doPause() {

    }

    public void onLeaderChanged(boolean isLeader) {
        this.isLeader = isLeader;
    }

    List<Goal> goals() {
        return new ArrayList<>(goalsByPriority);
    }

    public void validateReconfiguration(Map<String, Object> configs) throws ConfigException {
        try {
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS)) {
                long metricsDelay = ConfigUtils.getInteger(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS);
                if (metricsDelay <= 0) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS,
                            metricsDelay, "Max accepted metrics delay should be positive");
                }
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS)) {
                AutoBalancerControllerConfig tmp = new AutoBalancerControllerConfig(configs, false);
                tmp.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class);
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS)) {
                long detectInterval = ConfigUtils.getLong(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS);
                if (detectInterval < 0) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, detectInterval);
                }
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_CONCURRENCY)) {
                long concurrency = ConfigUtils.getInteger(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_CONCURRENCY);
                if (concurrency < 0) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_CONCURRENCY, concurrency);
                }
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS)) {
                AutoBalancerControllerConfig tmp = new AutoBalancerControllerConfig(configs, false);
                List<String> brokerIdStrs = tmp.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS);
                for (String brokerIdStr : brokerIdStrs) {
                    Integer.parseInt(brokerIdStr);
                }
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS)) {
                AutoBalancerControllerConfig tmp = new AutoBalancerControllerConfig(configs, false);
                tmp.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS);
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS)) {
                long interval = ConfigUtils.getLong(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
                if (interval < 0) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, interval);
                }
            }
            validateGoalsReconfiguration(configs);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException("Reconfiguration validation error " + e.getMessage());
        }
    }

    private void validateGoalsReconfiguration(Map<String, Object> configs) {
        for (Goal goal : this.goalsByPriority) {
            goal.validateReconfiguration(configs);
        }
    }

    public void reconfigure(Map<String, Object> configs) {
        configChangeLock.lock();
        try {
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS)) {
                this.maxTolerateMetricsDelayMs = ConfigUtils.getLong(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS);
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS)) {
                this.detectInterval = ConfigUtils.getLong(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS);
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_CONCURRENCY)) {
                this.executionConcurrency = ConfigUtils.getInteger(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_CONCURRENCY);
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS)) {
                AutoBalancerControllerConfig tmp = new AutoBalancerControllerConfig(configs, false);
                this.excludedBrokers = tmp.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS)
                        .stream().map(Integer::parseInt).collect(Collectors.toSet());
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS)) {
                AutoBalancerControllerConfig tmp = new AutoBalancerControllerConfig(configs, false);
                this.excludedTopics = new HashSet<>(tmp.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS));
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS)) {
                this.executionIntervalMs = ConfigUtils.getLong(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS)) {
                reconfigureGoals(configs);
            }
        } finally {
            configChangeLock.unlock();
        }
    }

    private void reconfigureGoals(Map<String, Object> configs) {
        AutoBalancerControllerConfig tmp = new AutoBalancerControllerConfig(configs, false);
        List<Goal> goals = tmp.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class);
        Collections.sort(goals);
        this.goalsByPriority = goals;
    }

    public void detect() {
        long nextExecutionDelay = detectInterval;
        try {
            nextExecutionDelay = detect0();
        } catch (Exception e) {
            logger.error("Detect error", e);
        } catch (Throwable t) {
            logger.error("Detect error and exit loop", t);
            return;
        }
        logger.info("Detect finished, next detect will be after {} ms", nextExecutionDelay);
        this.executorService.schedule(this::detect, nextExecutionDelay, TimeUnit.MILLISECONDS);
    }

    private boolean isRunnable() {
        return this.running.get() && this.isLeader;
    }

    long detect0() throws Exception {
        long detectInterval;
        Set<Integer> excludedBrokers;
        Set<String> excludedTopics;
        long maxTolerateMetricsDelayMs;
        int maxExecutionConcurrency;
        long executionIntervalMs;
        List<Goal> goals;
        configChangeLock.lock();
        try {
            detectInterval = this.detectInterval;
            excludedBrokers = new HashSet<>(this.excludedBrokers);
            excludedTopics = new HashSet<>(this.excludedTopics);
            maxTolerateMetricsDelayMs = this.maxTolerateMetricsDelayMs;
            maxExecutionConcurrency = this.executionConcurrency;
            executionIntervalMs = this.executionIntervalMs;
            goals = new ArrayList<>(this.goalsByPriority);
        } finally {
            configChangeLock.unlock();
        }
        if (!isRunnable()) {
            logger.info("not running, skip detect");
            return detectInterval;
        }
        logger.info("Start detect");
        // The delay in processing kraft log could result in outdated cluster snapshot
        ClusterModelSnapshot snapshot = this.clusterModel.snapshot(excludedBrokers, excludedTopics, maxTolerateMetricsDelayMs);
        snapshot.markSlowBrokers();

        Map<Integer, Boolean> slowBrokers = new HashMap<>();
        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            String brokerStr = logger.isDebugEnabled() ? broker.toString() : broker.shortString();
            slowBrokers.put(broker.getBrokerId(), broker.isSlowBroker());
            logger.info("Broker status: {}", brokerStr);
            if (logger.isDebugEnabled()) {
                for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica : snapshot.replicasFor(broker.getBrokerId())) {
                    logger.debug("Replica status {}", replica.shortString());
                }
            }
        }
        this.slowBrokers = slowBrokers;

        List<Action> totalActions = new ArrayList<>();
        Map<String, Set<String>> goalsByGroup = GoalUtils.groupGoals(goals);
        List<Goal> optimizedGoals = new ArrayList<>();
        for (Goal goal : goals) {
            if (!isRunnable()) {
                break;
            }
            totalActions.addAll(goal.optimize(snapshot, goals, optimizedGoals, goalsByGroup));
            optimizedGoals.add(goal);
        }
        if (!isRunnable()) {
            return detectInterval;
        }
        int totalActionSize = totalActions.size();
        List<List<Action>> actionsToExecute = checkAndGroupActions(totalActions, maxExecutionConcurrency, getTopicPartitionCount(snapshot));
        logger.info("Total actions num: {}, split to {} batches", totalActionSize, actionsToExecute.size());

        for (List<Action> batch : actionsToExecute) {
            this.actionExecutor.execute(batch).get();
            Thread.sleep(executionIntervalMs);
        }

        return detectInterval;
    }

    Map<String, Integer> getTopicPartitionCount(ClusterModelSnapshot snapshot) {
        Map<String, Integer> topicPartitionNumMap = new HashMap<>();
        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica : snapshot.replicasFor(broker.getBrokerId())) {
                topicPartitionNumMap.put(replica.getTopicPartition().topic(), topicPartitionNumMap
                        .getOrDefault(replica.getTopicPartition().topic(), 0) + 1);
            }
        }
        return topicPartitionNumMap;
    }

    List<List<Action>> checkAndGroupActions(List<Action> actions, int maxExecutionConcurrency, Map<String, Integer> topicPartitionNumMap) {
        List<Action> mergedActions = checkAndMergeActions(actions);
        List<List<Action>> groupedActions = new ArrayList<>();
        List<Action> batch = new ArrayList<>();

        Map<String, Integer> topicPartitionCountMap = new HashMap<>();
        Map<Integer, Integer> brokerActionConcurrencyMap = new HashMap<>();
        for (Action action : mergedActions) {
            int expectedPartitionCount = topicPartitionCountMap.getOrDefault(action.getSrcTopicPartition().topic(), 0) + 1;
            int expectedSrcBrokerConcurrency = brokerActionConcurrencyMap.getOrDefault(action.getSrcBrokerId(), 0) + 1;
            int expectedDestBrokerConcurrency = brokerActionConcurrencyMap.getOrDefault(action.getDestBrokerId(), 0) + 1;
            int partitionLimit = (int) Math.ceil(topicPartitionNumMap
                    .getOrDefault(action.getSrcTopicPartition().topic(), 0) * MAX_PARTITION_REASSIGNMENT_RATIO);
            if (expectedPartitionCount > partitionLimit || expectedSrcBrokerConcurrency > maxExecutionConcurrency
                    || expectedDestBrokerConcurrency > maxExecutionConcurrency) {
                groupedActions.add(batch);
                batch = new ArrayList<>();
                topicPartitionCountMap.clear();
                brokerActionConcurrencyMap.clear();
                expectedPartitionCount = 1;
                expectedSrcBrokerConcurrency = 1;
                expectedDestBrokerConcurrency = 1;
            }
            batch.add(action);
            topicPartitionCountMap.put(action.getSrcTopicPartition().topic(), expectedPartitionCount);
            brokerActionConcurrencyMap.put(action.getSrcBrokerId(), expectedSrcBrokerConcurrency);
            brokerActionConcurrencyMap.put(action.getDestBrokerId(), expectedDestBrokerConcurrency);
        }
        if (!batch.isEmpty()) {
            groupedActions.add(batch);
        }

        return groupedActions;
    }

    List<Action> checkAndMergeActions(List<Action> actions) throws IllegalStateException {
        List<Action> splitActions = new ArrayList<>();
        List<Action> mergedActions = new ArrayList<>();
        Map<TopicPartition, Action> actionMergeMap = new HashMap<>();

        for (Action action : actions) {
            if (action.getType() == ActionType.SWAP) {
                Action moveAction0 = new Action(ActionType.MOVE, action.getSrcTopicPartition(), action.getSrcBrokerId(), action.getDestBrokerId());
                Action moveAction1 = new Action(ActionType.MOVE, action.getDestTopicPartition(), action.getDestBrokerId(), action.getSrcBrokerId());
                splitActions.add(moveAction0);
                splitActions.add(moveAction1);
            } else {
                splitActions.add(action);
            }
        }

        for (Action action : splitActions) {
            Action prevAction = actionMergeMap.get(action.getSrcTopicPartition());
            if (prevAction == null) {
                mergedActions.add(action);
                actionMergeMap.put(action.getSrcTopicPartition(), action);
                continue;
            }
            if (prevAction.getDestBrokerId() != action.getSrcBrokerId()) {
                throw new IllegalStateException(String.format("Unmatched action chains for %s, prev: %s, next: %s",
                        action.getSrcTopicPartition(), prevAction, action));
            }
            prevAction.setDestBrokerId(action.getDestBrokerId());
        }
        mergedActions = mergedActions.stream().filter(action -> action.getSrcBrokerId() != action.getDestBrokerId())
                .collect(Collectors.toList());

        return mergedActions;
    }
}
