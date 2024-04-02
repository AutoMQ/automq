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

package kafka.autobalancer.executor;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.listeners.BrokerStatusListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ControllerActionExecutorService implements ActionExecutorService, Runnable, BrokerStatusListener {
    private final BlockingQueue<Action> actionQueue = new ArrayBlockingQueue<>(1000);
    private final Set<Integer> fencedBrokers = ConcurrentHashMap.newKeySet();
    private Logger logger;
    private Controller controller;
    private volatile long executionInterval;
    private KafkaThread dispatchThread;
    // TODO: optimize to per-broker concurrency control
    private long lastExecutionTime = 0L;
    private volatile boolean shutdown;

    public ControllerActionExecutorService(AutoBalancerControllerConfig config, Controller controller) {
        this(config, controller, null);
    }

    public ControllerActionExecutorService(AutoBalancerControllerConfig config, Controller controller, LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[ExecutionManager] ");
        }
        this.logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
        this.controller = controller;
        this.executionInterval = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
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
    public void execute(Action action) {
        try {
            this.actionQueue.put(action);
        } catch (InterruptedException ignored) {

        }
    }

    @Override
    public void execute(List<Action> actions) {
        for (Action action : actions) {
            execute(action);
        }
    }

    @Override
    public void validateReconfiguration(Map<String, Object> configs) throws ConfigException {
        try {
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS)) {
                long interval = ConfigUtils.getLong(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
                if (interval < 0) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS, interval);
                }
            }
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException("Reconfiguration validation error " + e.getMessage());
        }
    }

    @Override
    public void reconfigure(Map<String, Object> configs) {
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS)) {
            this.executionInterval = ConfigUtils.getLong(configs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS);
        }
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Action action = actionQueue.take();
                if (fencedBrokers.contains(action.getDestBrokerId())) {
                    logger.info("Broker {} is fenced, skip action {}", action.getDestBrokerId(), action);
                    continue;
                }
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
                doReassign(action);
                lastExecutionTime = Time.SYSTEM.milliseconds();
                logger.info("Executing {}", action.prettyString());
            } catch (InterruptedException ignored) {

            }
        }
    }

    private void doReassign(Action action) {
        ControllerRequestContext context = new ControllerRequestContext(null, null, OptionalLong.empty());
        AlterPartitionReassignmentsRequestData request = new AlterPartitionReassignmentsRequestData();
        List<AlterPartitionReassignmentsRequestData.ReassignableTopic> topicList = new ArrayList<>();
        topicList.add(buildTopic(action.getSrcTopicPartition(), action.getDestBrokerId()));
        if (action.getType() == ActionType.SWAP) {
            topicList.add(buildTopic(action.getDestTopicPartition(), action.getSrcBrokerId()));
        }
        request.setTopics(topicList);
        this.controller.alterPartitionReassignments(context, request);
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
}
