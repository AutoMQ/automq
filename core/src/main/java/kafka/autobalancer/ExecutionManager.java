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
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ExecutionManager implements Runnable {
    private final Logger logger;
    private final Controller controller;
    private final BlockingQueue<Action> actionQueue = new ArrayBlockingQueue<>(1000);
    private final long executionInterval;
    private final KafkaThread dispatchThread;
    // TODO: optimize to per-broker concurrency control
    private long lastExecutionTime = 0L;
    private volatile boolean shutdown;

    public ExecutionManager(AutoBalancerControllerConfig config, Controller controller) {
        this(config, controller, null);
    }

    public ExecutionManager(AutoBalancerControllerConfig config, Controller controller, LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[ExecutionManager] ");
        }
        this.logger = logContext.logger(ExecutionManager.class);
        this.controller = controller;
        this.executionInterval = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
        this.dispatchThread = KafkaThread.daemon("executor-dispatcher", this);
    }

    public void start() {
        this.shutdown = false;
        this.dispatchThread.start();
        logger.info("Started");
    }

    public void shutdown() {
        this.shutdown = true;
        this.dispatchThread.interrupt();
        logger.info("Shutdown completed");
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Action action = actionQueue.take();
                long now = System.currentTimeMillis();
                long nextExecutionTime = lastExecutionTime + executionInterval;
                while (!shutdown && lastExecutionTime != 0 && now < nextExecutionTime) {
                    try {
                        Thread.sleep(nextExecutionTime - now);
                    } catch (InterruptedException ignored) {
                        break;
                    }
                    now = System.currentTimeMillis();
                }
                if (shutdown) {
                    break;
                }
                ControllerRequestContext context = new ControllerRequestContext(null, null, OptionalLong.empty());
                AlterPartitionReassignmentsRequestData request = new AlterPartitionReassignmentsRequestData();
                List<AlterPartitionReassignmentsRequestData.ReassignableTopic> topicList = new ArrayList<>();
                topicList.add(buildTopic(action.getSrcTopicPartition(), action.getDestBrokerId()));
                if (action.getType() == ActionType.SWAP) {
                    topicList.add(buildTopic(action.getDestTopicPartition(), action.getSrcBrokerId()));
                }
                request.setTopics(topicList);
                this.controller.alterPartitionReassignments(context, request);
                lastExecutionTime = Time.SYSTEM.milliseconds();
                logger.info("Executing {}", action.prettyString());
            } catch (InterruptedException ignored) {

            }
        }
    }

    private AlterPartitionReassignmentsRequestData.ReassignableTopic buildTopic(TopicPartition tp, int brokerId) {
        String topicName = tp.topic();
        AlterPartitionReassignmentsRequestData.ReassignableTopic topic = new AlterPartitionReassignmentsRequestData.ReassignableTopic()
                .setName(topicName)
                .setPartitions(new ArrayList<>());
        AlterPartitionReassignmentsRequestData.ReassignablePartition partition = new AlterPartitionReassignmentsRequestData.ReassignablePartition();
        partition.setPartitionIndex(tp.partition());
        partition.setReplicas(List.of(brokerId));
        topic.setPartitions(List.of(partition));
        return topic;
    }

    public void appendActions(List<Action> actions) {
        for (Action action : actions) {
            try {
                this.actionQueue.put(action);
            } catch (InterruptedException ignored) {

            }
        }
    }
}
