/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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
