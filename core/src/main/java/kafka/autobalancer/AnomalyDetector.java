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

package kafka.autobalancer;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.goals.AbstractGoal;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.QuorumController;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AnomalyDetector {
    private final Logger logger;
    private final List<AbstractGoal> goalsByPriority;
    private final int maxActionsNumPerExecution;
    private final long executionInterval;
    private final ClusterModel clusterModel;
    private final ScheduledExecutorService executorService;
    private final ExecutionManager executionManager;
    private final long detectInterval;
    private final Set<Integer> excludedBrokers = new HashSet<>();
    private final Set<String> excludedTopics = new HashSet<>();
    private volatile boolean running;

    public AnomalyDetector(AutoBalancerControllerConfig config, QuorumController quorumController, ClusterModel clusterModel, LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[AnomalyDetector] ");
        }
        this.logger = logContext.logger(AnomalyDetector.class);
        this.goalsByPriority = config.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, AbstractGoal.class);
        this.maxActionsNumPerExecution = config.getInt(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS);
        Collections.sort(this.goalsByPriority);
        logger.info("Goals: {}", this.goalsByPriority);
        this.detectInterval = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS);
        this.executionInterval = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
        fetchExcludedConfig(config);
        this.clusterModel = clusterModel;
        this.executorService = Executors.newSingleThreadScheduledExecutor(new AutoBalancerThreadFactory("anomaly-detector"));
        this.executionManager = new ExecutionManager(config, quorumController,
                new LogContext(String.format("[ExecutionManager id=%d] ", quorumController.nodeId())));
        this.running = false;
    }

    private void fetchExcludedConfig(AutoBalancerControllerConfig config) {
        List<String> brokerIds = config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS);
        for (String brokerIdStr : brokerIds) {
            try {
                excludedBrokers.add(Integer.parseInt(brokerIdStr));
            } catch (NumberFormatException ignored) {

            }
        }
        List<String> topics = config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS);
        excludedTopics.addAll(topics);
        logger.info("Excluded brokers: {}, excluded topics: {}", excludedBrokers, excludedTopics);
    }

    public void start() {
        this.executionManager.start();
        this.executorService.schedule(this::detect, detectInterval, TimeUnit.MILLISECONDS);
        logger.info("Started");
    }

    public void shutdown() throws InterruptedException {
        this.running = false;
        this.executorService.shutdown();
        this.executionManager.shutdown();
        logger.info("Shutdown completed");
    }

    public void pause() {
        this.running = false;
    }

    public void resume() {
        this.running = true;
    }

    private void detect() {
        if (!this.running) {
            return;
        }
        logger.info("Start detect");
        // The delay in processing kraft log could result in outdated cluster snapshot
        ClusterModelSnapshot snapshot = this.clusterModel.snapshot(excludedBrokers, excludedTopics);

        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            logger.info("Broker status: {}", broker);
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica : snapshot.replicasFor(broker.getBrokerId())) {
                logger.debug("Replica status {}", replica);
            }
        }

        int availableActionNum = maxActionsNumPerExecution;
        for (AbstractGoal goal : goalsByPriority) {
            if (!this.running) {
                break;
            }
            List<Action> actions = goal.optimize(snapshot, goalsByPriority);
            int size = Math.min(availableActionNum, actions.size());
            this.executionManager.appendActions(actions.subList(0, size));
            availableActionNum -= size;
            if (availableActionNum <= 0) {
                break;
            }
        }

        long nextDelay = (maxActionsNumPerExecution - availableActionNum) * this.executionInterval + this.detectInterval;
        this.executorService.schedule(this::detect, nextDelay, TimeUnit.MILLISECONDS);
        logger.info("Detect finished, next detect will be after {} ms", nextDelay);
    }
}
