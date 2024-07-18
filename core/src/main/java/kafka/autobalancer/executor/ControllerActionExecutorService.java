/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.executor;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.listeners.BrokerStatusListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ControllerActionExecutorService implements ActionExecutorService, Runnable, BrokerStatusListener {
    private final BlockingQueue<Task> actionQueue = new ArrayBlockingQueue<>(1000);
    private final Set<Integer> fencedBrokers = ConcurrentHashMap.newKeySet();
    private final Logger logger;
    private final Controller controller;
    private final KafkaThread dispatchThread;
    private volatile boolean shutdown;

    public ControllerActionExecutorService(Controller controller) {
        this(controller, null);
    }

    public ControllerActionExecutorService(Controller controller, LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[ExecutionManager] ");
        }
        this.logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
        this.controller = controller;
        this.dispatchThread = KafkaThread.daemon("executor-dispatcher", this);
    }

    @Override
    public void start() {
        this.shutdown = false;
        this.dispatchThread.start();
        logger.info("Started");
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        this.dispatchThread.interrupt();
        logger.info("Shutdown completed");
    }

    @Override
    public CompletableFuture<Void> execute(List<Action> actions) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        try {
            actionQueue.put(new Task(actions, cf));
        } catch (InterruptedException e) {
            logger.error("Failed to put actions into queue", e);
            cf.completeExceptionally(e);
        }
        return cf;
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                doReassign(actionQueue.take());
            } catch (InterruptedException ignored) {

            }
        }
    }

    private void doReassign(Task task) {
        ControllerRequestContext context = new ControllerRequestContext(null, null, OptionalLong.empty());
        AlterPartitionReassignmentsRequestData request = new AlterPartitionReassignmentsRequestData();
        List<AlterPartitionReassignmentsRequestData.ReassignableTopic> topicList = new ArrayList<>();

        Map<String, List<AlterPartitionReassignmentsRequestData.ReassignablePartition>> topicPartitionMap = new HashMap<>();

        for (Action action : task.actions) {
            if (fencedBrokers.contains(action.getDestBrokerId())) {
                logger.info("Broker {} is fenced, skip action {}", action.getDestBrokerId(), action);
                continue;
            }
            addTopicPartition(topicPartitionMap, action.getSrcTopicPartition(), action.getDestBrokerId());
            if (action.getType() == ActionType.SWAP) {
                addTopicPartition(topicPartitionMap, action.getDestTopicPartition(), action.getSrcBrokerId());
            }
            logger.info("Executing: {}", action.prettyString());
        }
        for (Map.Entry<String, List<AlterPartitionReassignmentsRequestData.ReassignablePartition>> entry : topicPartitionMap.entrySet()) {
            AlterPartitionReassignmentsRequestData.ReassignableTopic topic = new AlterPartitionReassignmentsRequestData.ReassignableTopic()
                    .setName(entry.getKey());
            topic.setPartitions(entry.getValue());
            topicList.add(topic);
        }
        request.setTopics(topicList);
        this.controller.alterPartitionReassignments(context, request).whenComplete((response, exception) -> {
            if (exception != null) {
                logger.error("Failed to alter partition reassignments", exception);
                task.getFuture().completeExceptionally(exception);
            } else {
                handleResponse(response, task.getFuture());
            }
        });
    }

    private void handleResponse(AlterPartitionReassignmentsResponseData response, CompletableFuture<Void> future) {
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            future.completeExceptionally(new ApiException("Failed to alter partition reassignments", topLevelError.exception()));
        } else {
            for (AlterPartitionReassignmentsResponseData.ReassignableTopicResponse topicResponse : response.responses()) {
                for (AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse partitionResponse : topicResponse.partitions()) {
                    Errors partitionError = Errors.forCode(partitionResponse.errorCode());
                    if (partitionError != Errors.NONE) {
                        future.completeExceptionally(new ApiException(String.format("Failed to alter partition %s-%d reassignments",
                                topicResponse.name(), partitionResponse.partitionIndex()), partitionError.exception()));
                    }
                }

            }
            future.complete(null);
        }
    }

    private void addTopicPartition(Map<String, List<AlterPartitionReassignmentsRequestData.ReassignablePartition>> topicPartitionMap,
                                   TopicPartition tp, int brokerId) {
        List<AlterPartitionReassignmentsRequestData.ReassignablePartition> partitions = topicPartitionMap
                .computeIfAbsent(tp.topic(), k -> new ArrayList<>());
        partitions.add(buildPartition(tp.partition(), brokerId));
    }

    private AlterPartitionReassignmentsRequestData.ReassignablePartition buildPartition(int partitionIndex, int brokerId) {
        AlterPartitionReassignmentsRequestData.ReassignablePartition partition = new AlterPartitionReassignmentsRequestData.ReassignablePartition();
        partition.setPartitionIndex(partitionIndex);
        partition.setReplicas(List.of(brokerId));
        return partition;
    }

    @Override
    public void onBrokerRegister(RegisterBrokerRecord record) {
        fencedBrokers.remove(record.brokerId());
    }

    @Override
    public void onBrokerUnregister(UnregisterBrokerRecord record) {
        fencedBrokers.add(record.brokerId());
    }

    @Override
    public void onBrokerRegistrationChanged(BrokerRegistrationChangeRecord record) {
        boolean fenced = record.fenced() == BrokerRegistrationFencingChange.FENCE.value()
                || record.inControlledShutdown() == BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value();
        if (fenced) {
            fencedBrokers.add(record.brokerId());
        } else {
            fencedBrokers.remove(record.brokerId());
        }
    }

    private static class Task {
        private final List<Action> actions;
        private final CompletableFuture<Void> future;

        public Task(List<Action> actions, CompletableFuture<Void> future) {
            this.actions = actions;
            this.future = future;
        }

        public List<Action> getActions() {
            return actions;
        }

        public CompletableFuture<Void> getFuture() {
            return future;
        }
    }
}
