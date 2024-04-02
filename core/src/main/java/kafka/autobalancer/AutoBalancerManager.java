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
import kafka.autobalancer.detector.AnomalyDetector;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.detector.AnomalyDetectorBuilder;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.ClusterStatusListenerRegistry;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;
import kafka.autobalancer.executor.ControllerActionExecutorService;
import kafka.autobalancer.model.RecordClusterModel;
import kafka.autobalancer.services.AutoBalancerService;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AutoBalancerManager extends AutoBalancerService {
    protected final Time time;
    protected final Map<?, ?> configs;
    protected final QuorumController quorumController;
    protected final KafkaRaftClient<ApiMessageAndVersion> raftClient;
    protected LoadRetriever loadRetriever;
    protected AnomalyDetector anomalyDetector;
    protected ActionExecutorService actionExecutorService;
    protected volatile boolean enabled;

    public AutoBalancerManager(Time time, Map<?, ?> configs, QuorumController quorumController, KafkaRaftClient<ApiMessageAndVersion> raftClient) {
        super(new LogContext(String.format("[AutoBalancerManager id=%d] ", quorumController.nodeId())));
        this.time = time;
        this.configs = configs;
        this.quorumController = quorumController;
        this.raftClient = raftClient;
        init();
    }

    public void init() {
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(configs, false);
        this.enabled = config.getBoolean(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE);
        RecordClusterModel clusterModel = new RecordClusterModel(new LogContext(String.format("[ClusterModel id=%d] ", quorumController.nodeId())));
        this.loadRetriever = new LoadRetriever(config, quorumController, clusterModel,
                new LogContext(String.format("[LoadRetriever id=%d] ", quorumController.nodeId())));
        this.actionExecutorService = new ControllerActionExecutorService(config, quorumController,
                new LogContext(String.format("[ExecutionManager id=%d] ", quorumController.nodeId())));
        this.actionExecutorService.start();

        this.anomalyDetector = new AnomalyDetectorBuilder()
                .logContext(new LogContext(String.format("[AnomalyDetector id=%d] ", quorumController.nodeId())))
                .maxActionsNumPerExecution(config.getInt(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS))
                .detectIntervalMs(config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS))
                .maxTolerateMetricsDelayMs(config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS))
                .coolDownIntervalPerActionMs(config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS))
                .clusterModel(clusterModel)
                .executor(this.actionExecutorService)
                .addGoals(config.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class))
                .excludedBrokers(config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS)
                        .stream().map(Integer::parseInt).collect(Collectors.toSet()))
                .excludedTopics(config.getList(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS))
                .build();

        ClusterStatusListenerRegistry registry = new ClusterStatusListenerRegistry();
        registry.register((BrokerStatusListener) clusterModel);
        registry.register((TopicPartitionStatusListener) clusterModel);
        registry.register((BrokerStatusListener) this.actionExecutorService);
        registry.register(this.loadRetriever);
        raftClient.register(new AutoBalancerListener(quorumController.nodeId(), time, registry, this.loadRetriever, this.anomalyDetector));
    }

    @Override
    protected void doStart() {
        loadRetriever.start();
        anomalyDetector.start();
    }

    @Override
    protected void doShutdown() {
        try {
            anomalyDetector.shutdown();
            actionExecutorService.shutdown();
            loadRetriever.shutdown();
        } catch (Exception e) {
            logger.error("Shutdown exception", e);
        }
    }

    @Override
    protected void doPause() {
        this.loadRetriever.pause();
        this.anomalyDetector.pause();
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return AutoBalancerControllerConfig.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        Map<String, Object> objectConfigs = new HashMap<>(configs);
        try {
            if (objectConfigs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE)) {
                ConfigUtils.getBoolean(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE);
            }
            this.anomalyDetector.validateReconfiguration(objectConfigs);
            this.actionExecutorService.validateReconfiguration(objectConfigs);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException("Reconfiguration validation error " + e.getMessage());
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        Map<String, Object> objectConfigs = new HashMap<>(configs);
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE)) {
            boolean isEnable = ConfigUtils.getBoolean(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE);
            if (!this.enabled && isEnable) {
                this.enabled = true;
                this.start();
                logger.info("AutoBalancerManager resumed.");
            } else if (this.enabled && !isEnable) {
                this.enabled = false;
                this.pause();
                logger.info("AutoBalancerManager paused.");
            }
        }
        this.anomalyDetector.reconfigure(objectConfigs);
        this.actionExecutorService.reconfigure(objectConfigs);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
