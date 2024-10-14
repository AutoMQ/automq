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
import kafka.autobalancer.detector.AnomalyDetectorImpl;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.ClusterStatusListenerRegistry;
import kafka.autobalancer.listeners.LeaderChangeListener;
import kafka.autobalancer.model.ClusterModel;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.raft.LeaderAndEpoch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.OptionalInt;

public class AutoBalancerListenerTest {
    @Test
    public void testReconfigure() {
        ClusterStatusListenerRegistry registry = new ClusterStatusListenerRegistry();
        AnomalyDetectorImpl anomalyDetector = new AnomalyDetectorImpl(Collections.emptyMap(), null, Mockito.mock(ClusterModel.class), Mockito.mock(ActionExecutorService.class));
        LoadRetriever loadRetriever = new LoadRetriever(Mockito.mock(AutoBalancerControllerConfig.class), Mockito.mock(Controller.class), Mockito.mock(ClusterModel.class));

        Assertions.assertFalse(anomalyDetector.isLeader());
        Assertions.assertFalse(loadRetriever.isLeader());

        registry.register(anomalyDetector);
        registry.register((BrokerStatusListener) loadRetriever);
        registry.register((LeaderChangeListener) loadRetriever);
        AutoBalancerListener autoBalancerListener = new AutoBalancerListener(0, Time.SYSTEM, registry);
        autoBalancerListener.handleLeaderChange0(new LeaderAndEpoch(OptionalInt.of(0), 0));
        Assertions.assertTrue(anomalyDetector.isLeader());
        Assertions.assertTrue(loadRetriever.isLeader());
    }
}
