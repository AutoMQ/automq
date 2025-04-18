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

package kafka.autobalancer;

import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.detector.AbstractAnomalyDetector;
import kafka.autobalancer.detector.AnomalyDetectorImpl;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.executor.ControllerActionExecutorService;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.ClusterStatusListenerRegistry;
import kafka.autobalancer.listeners.LeaderChangeListener;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;
import kafka.autobalancer.model.RecordClusterModel;
import kafka.autobalancer.services.AutoBalancerService;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import com.automq.stream.utils.LogContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AutoBalancerManager extends AutoBalancerService {
    protected final Time time;
    protected final Map<?, ?> configs;
    protected final QuorumController quorumController;
    protected final KafkaRaftClient<ApiMessageAndVersion> raftClient;
    protected final List<Reconfigurable> reconfigurables = new ArrayList<>();
    protected LoadRetriever loadRetriever;
    protected AbstractAnomalyDetector anomalyDetector;
    protected ActionExecutorService actionExecutorService;
    protected volatile boolean enabled;

    public AutoBalancerManager(Time time, Map<?, ?> configs, QuorumController quorumController, KafkaRaftClient<ApiMessageAndVersion> raftClient) {
        super(new LogContext(String.format("[AutoBalancerManager id=%d] ", quorumController.nodeId())));
        this.time = time;
        this.configs = configs;
        this.quorumController = quorumController;
        this.raftClient = raftClient;
        init();
        if (enabled) {
            run();
        }
    }

    protected void init() {
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(configs, false);
        this.enabled = config.getBoolean(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE);
        int nodeId = quorumController.nodeId();
        RecordClusterModel clusterModel = new RecordClusterModel(new LogContext(String.format("[ClusterModel id=%d] ", nodeId)));
        this.loadRetriever = new LoadRetriever(config, quorumController, clusterModel,
                new LogContext(String.format("[LoadRetriever id=%d] ", nodeId)));
        this.actionExecutorService = new ControllerActionExecutorService(quorumController,
                new LogContext(String.format("[ExecutionManager id=%d] ", nodeId)));
        this.actionExecutorService.start();

        this.anomalyDetector = new AnomalyDetectorImpl(config.originals(),
            new LogContext(String.format("[AnomalyDetector id=%d] ", nodeId)), clusterModel, actionExecutorService);
        ((AnomalyDetectorImpl) this.anomalyDetector).lockedNodes(() -> quorumController.nodeControlManager().lockedNodes());

        this.reconfigurables.add(anomalyDetector);

        ClusterStatusListenerRegistry registry = new ClusterStatusListenerRegistry();
        registry.register((BrokerStatusListener) clusterModel);
        registry.register((TopicPartitionStatusListener) clusterModel);
        registry.register((BrokerStatusListener) this.actionExecutorService);
        registry.register((LeaderChangeListener) this.anomalyDetector);
        registry.register((BrokerStatusListener) this.loadRetriever);
        registry.register((LeaderChangeListener) this.loadRetriever);
        raftClient.register(new AutoBalancerListener(nodeId, time, registry));
    }

    @Override
    protected void doRun() {
        loadRetriever.run();
        anomalyDetector.run();
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
            for (Reconfigurable reconfigurable : reconfigurables) {
                reconfigurable.validateReconfiguration(objectConfigs);
            }
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
                this.run();
                logger.info("AutoBalancerManager resumed.");
            } else if (this.enabled && !isEnable) {
                this.enabled = false;
                this.pause();
                logger.info("AutoBalancerManager paused.");
            }
        }
        for (Reconfigurable reconfigurable : reconfigurables) {
            reconfigurable.reconfigure(objectConfigs);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
