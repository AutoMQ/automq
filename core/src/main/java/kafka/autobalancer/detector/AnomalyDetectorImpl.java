/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.detector;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.goals.GoalUtils;
import kafka.autobalancer.listeners.LeaderChangeListener;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.controller.es.ClusterStats;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;

import com.automq.stream.utils.LogContext;

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
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AnomalyDetectorImpl extends AbstractAnomalyDetector implements LeaderChangeListener {
    private static final double MAX_PARTITION_REASSIGNMENT_RATIO = 0.5;
    private static final int MAX_REASSIGNMENT_SOURCE_NODE_COUNT = 10;
    private static final long MAX_REASSIGNMENT_EXECUTION_TIME_MS = 60000;
    private final ClusterModel clusterModel;
    private final ScheduledExecutorService executorService;
    private final ActionExecutorService actionExecutor;
    private final Lock configChangeLock = new ReentrantLock();
    private List<Goal> goalsByPriority;
    private Set<Integer> excludedBrokers;
    private Supplier<Set<Integer>> lockedNodes = Collections::emptySet;
    private Set<String> excludedTopics;
    private long detectInterval;
    private long maxTolerateMetricsDelayMs;
    private int executionConcurrency;
    private long executionIntervalMs;
    private volatile boolean isLeader = false;
    private volatile Map<Integer, Boolean> slowBrokers = new HashMap<>();

    public AnomalyDetectorImpl(Map<String, ?> configs, LogContext logContext, ClusterModel clusterModel, ActionExecutorService actionExecutor) {
        super(logContext);
        this.configure(configs);
        this.clusterModel = clusterModel;
        this.actionExecutor = actionExecutor;
        this.executorService = Executors.newScheduledThreadPool(2, new AutoBalancerThreadFactory("anomaly-detector"));
        Collections.sort(this.goalsByPriority);
        this.executorService.schedule(this::detect, detectInterval, TimeUnit.MILLISECONDS);
        this.executorService.scheduleAtFixedRate(() -> {
            if (isRunning()) {
                updateClusterStats(clusterModel, maxTolerateMetricsDelayMs);
            }
        }, 30, 30, TimeUnit.SECONDS);
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

    public void updateClusterStats(ClusterModel clusterModel, long maxTolerateMetricsDelayMs) {
        ClusterModel.ClusterLoad clusterLoad = clusterModel.getClusterLoad(maxTolerateMetricsDelayMs);
        ClusterStats.getInstance().updateBrokerLoads(clusterLoad.brokerLoads());
        ClusterStats.getInstance().updatePartitionLoads(clusterLoad.partitionLoads());
        ClusterStats.getInstance().updateExcludedBrokers(getExcludedBrokers());
    }

    public Set<Integer> getExcludedBrokers() {
        return this.excludedBrokers;
    }

    public boolean isLeader() {
        return this.isLeader;
    }

    @Override
    public void onLeaderChanged(boolean isLeader) {
        this.isLeader = isLeader;
    }

    List<Goal> goals() {
        return new ArrayList<>(goalsByPriority);
    }

    @Override
    public void configure(Map<String, ?> rawConfigs) {
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(rawConfigs, false);
        this.maxTolerateMetricsDelayMs = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS);
        this.detectInterval = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS);
        this.executionConcurrency = config.getInt(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_CONCURRENCY);
        this.excludedBrokers = config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS)
            .stream().map(Integer::parseInt).collect(Collectors.toSet());
        ClusterStats.getInstance().updateExcludedBrokers(this.excludedBrokers);
        this.excludedTopics = new HashSet<>(config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS));
        this.executionIntervalMs = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
        List<Goal> goals = config.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class);
        Collections.sort(goals);
        this.goalsByPriority = goals;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        // NOT USED
        return Collections.emptySet();
    }

    @SuppressWarnings("NPathComplexity")
    @Override
    public void validateReconfiguration(Map<String, ?> rawConfigs) throws ConfigException {
        Map<String, Object> configs = new HashMap<>(rawConfigs);
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

    @Override
    public void reconfigure(Map<String, ?> rawConfigs) {
        Map<String, Object> configs = new HashMap<>(rawConfigs);
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
                ClusterStats.getInstance().updateExcludedBrokers(this.excludedBrokers);
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
            logger.info("Reconfigured anomaly detector with detectInterval: {}ms, executionConcurrency: {}, executionIntervalMs: {}ms, goals: {}, excluded brokers: {}, excluded topics: {}",
                    this.detectInterval, this.executionConcurrency, this.executionIntervalMs, this.goalsByPriority, this.excludedBrokers, this.excludedTopics);
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

    public void lockedNodes(Supplier<Set<Integer>> lockedNodes) {
        this.lockedNodes = lockedNodes;
    }

    private boolean isRunnable() {
        return this.running.get() && this.isLeader;
    }

    @SuppressWarnings("NPathComplexity")
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
            excludedBrokers.addAll(lockedNodes.get());
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
        Map<Integer, BrokerUpdater.Broker> brokersBefore = new HashMap<>();
        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            brokersBefore.put(broker.getBrokerId(), broker.copy());
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
        goals.forEach(goal -> goal.initialize(snapshot));
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

        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            if (brokersBefore.containsKey(broker.getBrokerId())) {
                BrokerUpdater.Broker beforeBroker = brokersBefore.get(broker.getBrokerId());
                logger.info("Expected load change: brokerId={}, {}", broker.getBrokerId(), broker.deltaLoadString(beforeBroker));
            }
        }

        int totalActionSize = totalActions.size();
        List<List<Action>> actionsToExecute = checkAndGroupActions(totalActions, maxExecutionConcurrency,
            MAX_PARTITION_REASSIGNMENT_RATIO, MAX_REASSIGNMENT_SOURCE_NODE_COUNT, getTopicPartitionCount(snapshot));
        logger.info("Total actions num: {}, split to {} batches, estimated time to complete: {}ms", totalActionSize,
            actionsToExecute.size(), Math.max(0, actionsToExecute.size() - 1) * executionIntervalMs);

        long start = System.currentTimeMillis();
        for (int i = 0; i < actionsToExecute.size(); i++) {
            if (System.currentTimeMillis() - start > MAX_REASSIGNMENT_EXECUTION_TIME_MS) {
                logger.warn("Exceeds max reassignment execution time, stop executing actions");
                break;
            }
            if (i != 0) {
                Thread.sleep(executionIntervalMs);
            }
            this.actionExecutor.execute(actionsToExecute.get(i)).get();
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

    List<List<Action>> checkAndGroupActions(List<Action> actions, int maxExecutionConcurrency, double maxPartitionReassignmentRatio,
        int maxNodeReassignmentConcurrency, Map<String, Integer> topicPartitionNumMap) {
        if (actions.isEmpty()) {
            return Collections.emptyList();
        }
        List<Action> mergedActions = checkAndMergeActions(actions);
        List<List<Action>> groupedActions = new ArrayList<>();
        List<Action> batch = new ArrayList<>();

        // topic -> number of partitions to be reassigned for this topic
        Map<String, Integer> topicPartitionCountMap = new HashMap<>();
        // broker -> number of partitions to be reassigned to or from this broker
        Map<Integer, Integer> brokerActionConcurrencyMap = new HashMap<>();
        // broker -> broker ids of nodes that have partitions to be reassigned to this broker
        Map<Integer, Set<Integer>> brokerNodeConcurrencyMap = new HashMap<>();
        for (Action action : mergedActions) {
            int currPartitionCount = topicPartitionCountMap.getOrDefault(action.getSrcTopicPartition().topic(), 0);
            int currSrcBrokerConcurrency = brokerActionConcurrencyMap.getOrDefault(action.getSrcBrokerId(), 0);
            int currDestBrokerConcurrency = brokerActionConcurrencyMap.getOrDefault(action.getDestBrokerId(), 0);
            Set<Integer> currNodeIds = brokerNodeConcurrencyMap.getOrDefault(action.getDestBrokerId(), new HashSet<>());
            int partitionLimit = (int) Math.ceil(topicPartitionNumMap
                    .getOrDefault(action.getSrcTopicPartition().topic(), 0) * maxPartitionReassignmentRatio);
            if (currPartitionCount >= partitionLimit // exceeds the maximum number of partitions that can be reassigned concurrently for one topic
                || currSrcBrokerConcurrency >= maxExecutionConcurrency // exceeds the maximum number of partitions that can be reassigned concurrently to one broker
                || currDestBrokerConcurrency >= maxExecutionConcurrency // exceeds the maximum number of partitions that can be reassigned concurrently from one broker
                || currNodeIds.size() >= maxNodeReassignmentConcurrency) { // exceeds the maximum number of different nodes that have partitions to be reassigned to this broker
                groupedActions.add(batch);
                batch = new ArrayList<>();
                topicPartitionCountMap.clear();
                brokerActionConcurrencyMap.clear();
                currPartitionCount = 0;
                currSrcBrokerConcurrency = 0;
                currDestBrokerConcurrency = 0;
                currNodeIds = new HashSet<>();
            }
            batch.add(action);
            topicPartitionCountMap.put(action.getSrcTopicPartition().topic(), currPartitionCount + 1);
            brokerActionConcurrencyMap.put(action.getSrcBrokerId(), currSrcBrokerConcurrency + 1);
            brokerActionConcurrencyMap.put(action.getDestBrokerId(), currDestBrokerConcurrency + 1);
            currNodeIds.add(action.getSrcBrokerId());
            brokerNodeConcurrencyMap.put(action.getDestBrokerId(), currNodeIds);
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
