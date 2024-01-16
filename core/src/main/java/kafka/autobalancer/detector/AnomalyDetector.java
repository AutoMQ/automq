/*
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

package kafka.autobalancer.detector;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
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
    private final boolean aggregateBrokerLoad;
    private volatile boolean running;

    AnomalyDetector(LogContext logContext, int maxActionsNumPerDetect, long detectIntervalMs, long maxTolerateMetricsDelayMs,
                    long coolDownIntervalPerActionMs, boolean aggregateBrokerLoad, ClusterModel clusterModel,
                    ActionExecutorService actionExecutor, List<Goal> goals,
                    Set<Integer> excludedBrokers, Set<String> excludedTopics) {
        this.logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);

        this.maxActionsNumPerExecution = maxActionsNumPerDetect;
        this.detectInterval = detectIntervalMs;
        this.maxTolerateMetricsDelayMs = maxTolerateMetricsDelayMs;
        this.coolDownIntervalPerActionMs = coolDownIntervalPerActionMs;
        this.aggregateBrokerLoad = aggregateBrokerLoad;
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

    void detect() {
        if (!this.running) {
            return;
        }
        logger.info("Start detect");
        // The delay in processing kraft log could result in outdated cluster snapshot
        ClusterModelSnapshot snapshot = this.clusterModel.snapshot(excludedBrokers, excludedTopics,
                this.maxTolerateMetricsDelayMs, this.aggregateBrokerLoad);

        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            logger.info("Broker status: {}", broker);
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica : snapshot.replicasFor(broker.getBrokerId())) {
                logger.debug("Replica status {}", replica);
            }
        }

        int availableActionNum = maxActionsNumPerExecution;
        for (Goal goal : goalsByPriority) {
            if (!this.running) {
                break;
            }
            List<Action> actions = goal.optimize(snapshot, goalsByPriority);
            int size = Math.min(availableActionNum, actions.size());
            this.actionExecutor.execute(actions.subList(0, size));
            availableActionNum -= size;
            if (availableActionNum <= 0) {
                logger.info("No more action can be executed in this round");
                break;
            }
        }

        long nextDelay = (maxActionsNumPerExecution - availableActionNum) * this.coolDownIntervalPerActionMs + this.detectInterval;
        this.executorService.schedule(this::detect, nextDelay, TimeUnit.MILLISECONDS);
        logger.info("Detect finished, next detect will be after {} ms", nextDelay);
    }
}
