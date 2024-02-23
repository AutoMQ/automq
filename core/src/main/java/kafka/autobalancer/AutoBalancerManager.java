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

package kafka.autobalancer;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.detector.AnomalyDetector;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.detector.AnomalyDetectorBuilder;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.ClusterStatusListenerRegistry;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;
import kafka.autobalancer.executor.ControllerActionExecutorService;
import kafka.autobalancer.model.RecordClusterModel;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class AutoBalancerManager implements AutoBalancerService {
    private final Logger logger;
    private final LoadRetriever loadRetriever;
    private final AnomalyDetector anomalyDetector;
    private final KafkaEventQueue queue;

    public AutoBalancerManager(Time time, KafkaConfig kafkaConfig, QuorumController quorumController, KafkaRaftClient<ApiMessageAndVersion> raftClient) {
        LogContext logContext = new LogContext(String.format("[AutoBalancerManager id=%d] ", quorumController.nodeId()));
        logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(kafkaConfig.props(), false);
        RecordClusterModel clusterModel = new RecordClusterModel(new LogContext(String.format("[ClusterModel id=%d] ", quorumController.nodeId())));
        this.loadRetriever = new LoadRetriever(config, quorumController, clusterModel,
                new LogContext(String.format("[LoadRetriever id=%d] ", quorumController.nodeId())));
        ControllerActionExecutorService actionExecutorService = new ControllerActionExecutorService(config, quorumController,
                new LogContext(String.format("[ExecutionManager id=%d] ", quorumController.nodeId())));

        this.anomalyDetector = new AnomalyDetectorBuilder()
                .logContext(new LogContext(String.format("[AnomalyDetector id=%d] ", quorumController.nodeId())))
                .maxActionsNumPerExecution(config.getInt(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS))
                .detectIntervalMs(config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS))
                .maxTolerateMetricsDelayMs(config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS))
                .coolDownIntervalPerActionMs(config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS))
                .clusterModel(clusterModel)
                .executor(actionExecutorService)
                .addGoals(config.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class))
                .excludedBrokers(parseExcludedBrokers(config))
                .excludedTopics(config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS))
                .build();

        this.queue = new KafkaEventQueue(time, new org.apache.kafka.common.utils.LogContext(), "auto-balancer-");
        ClusterStatusListenerRegistry registry = new ClusterStatusListenerRegistry();
        registry.register((BrokerStatusListener) clusterModel);
        registry.register((TopicPartitionStatusListener) clusterModel);
        registry.register(actionExecutorService);
        registry.register(this.loadRetriever);
        raftClient.register(new AutoBalancerListener(quorumController.nodeId(), logContext, queue, registry, this.loadRetriever, this.anomalyDetector));
    }

    @Override
    public void start() {
        loadRetriever.start();
        anomalyDetector.start();
        logger.info("Started");
    }

    @Override
    public void shutdown() {
        try {
            anomalyDetector.shutdown();
            loadRetriever.shutdown();
            queue.close();
        } catch (Exception e) {
            logger.error("Exception in shutdown", e);
        }

        logger.info("Shutdown completed");
    }

    private Set<Integer> parseExcludedBrokers(AutoBalancerControllerConfig config) {
        Set<Integer> excludedBrokers = new HashSet<>();
        for (String brokerId : config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS)) {
            try {
                excludedBrokers.add(Integer.parseInt(brokerId));
            } catch (Exception e) {
                logger.warn("Failed to parse broker id {} from config", brokerId);
            }

        }
        return excludedBrokers;
    }
}
