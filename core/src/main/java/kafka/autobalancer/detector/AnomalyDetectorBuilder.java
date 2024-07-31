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

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.model.ClusterModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AnomalyDetectorBuilder {
    private final List<Goal> goalsByPriority = new ArrayList<>();
    private final Set<Integer> excludedBrokers = new HashSet<>();
    private final Set<String> excludedTopics = new HashSet<>();
    private LogContext logContext = null;
    private ClusterModel clusterModel = null;
    private ActionExecutorService executor = null;
    private long detectIntervalMs = 60000;
    private long maxTolerateMetricsDelayMs = 30000;
    private int executionConcurrency = 50;
    private long executionIntervalMs = 5000;

    public AnomalyDetectorBuilder() {

    }

    public AnomalyDetectorBuilder logContext(LogContext logContext) {
        this.logContext = logContext;
        return this;
    }

    public AnomalyDetectorBuilder addGoal(Goal goal) {
        this.goalsByPriority.add(goal);
        return this;
    }

    public AnomalyDetectorBuilder addGoals(List<Goal> goals) {
        this.goalsByPriority.addAll(goals);
        return this;
    }

    public AnomalyDetectorBuilder excludedBroker(Integer excludedBroker) {
        this.excludedBrokers.add(excludedBroker);
        return this;
    }

    public AnomalyDetectorBuilder excludedBrokers(Collection<Integer> excludedBrokers) {
        this.excludedBrokers.addAll(excludedBrokers);
        return this;
    }

    public AnomalyDetectorBuilder excludedTopic(String excludedTopic) {
        this.excludedTopics.add(excludedTopic);
        return this;
    }

    public AnomalyDetectorBuilder excludedTopics(Collection<String> excludedTopics) {
        this.excludedTopics.addAll(excludedTopics);
        return this;
    }

    public AnomalyDetectorBuilder clusterModel(ClusterModel clusterModel) {
        this.clusterModel = clusterModel;
        return this;
    }

    public AnomalyDetectorBuilder executor(ActionExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public AnomalyDetectorBuilder detectIntervalMs(long detectIntervalMs) {
        this.detectIntervalMs = detectIntervalMs;
        return this;
    }

    public AnomalyDetectorBuilder maxTolerateMetricsDelayMs(long maxTolerateMetricsDelayMs) {
        this.maxTolerateMetricsDelayMs = maxTolerateMetricsDelayMs;
        return this;
    }

    public AnomalyDetectorBuilder executionConcurrency(int executionConcurrency) {
        this.executionConcurrency = executionConcurrency;
        return this;
    }

    public AnomalyDetectorBuilder executionIntervalMs(long executionIntervalMs) {
        this.executionIntervalMs = executionIntervalMs;
        return this;
    }

    public AnomalyDetector build() {
        if (logContext == null) {
            logContext = new LogContext("[AnomalyDetector] ");
        }
        if (clusterModel == null) {
            throw new IllegalArgumentException("ClusterModel must be set");
        }
        if (executor == null) {
            throw new IllegalArgumentException("Executor must be set");
        }
        if (goalsByPriority.isEmpty()) {
            throw new IllegalArgumentException("At least one goal must be set");
        }
        return new AnomalyDetector(logContext, detectIntervalMs, maxTolerateMetricsDelayMs, executionConcurrency,
                executionIntervalMs, clusterModel, executor, goalsByPriority, excludedBrokers, excludedTopics);
    }
}
