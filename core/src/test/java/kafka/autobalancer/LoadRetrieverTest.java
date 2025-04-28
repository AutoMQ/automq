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
import kafka.autobalancer.model.ClusterModel;

import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class LoadRetrieverTest {

    @Test
    public void testBrokerChanged() {
        LoadRetriever loadRetriever = Mockito.spy(new LoadRetriever(Mockito.mock(AutoBalancerControllerConfig.class), Mockito.mock(Controller.class), Mockito.mock(ClusterModel.class)));
        loadRetriever.onBrokerRegister(new RegisterBrokerRecord().setBrokerId(0).setFenced(false).setEndPoints(
            new RegisterBrokerRecord.BrokerEndpointCollection(List.of(
                new RegisterBrokerRecord.BrokerEndpoint().setHost("192.168.0.0").setPort(9092)).iterator())));
        loadRetriever.onBrokerRegister(new RegisterBrokerRecord().setBrokerId(1).setFenced(false).setEndPoints(
            new RegisterBrokerRecord.BrokerEndpointCollection(List.of(
                new RegisterBrokerRecord.BrokerEndpoint().setHost("192.168.0.1").setPort(9093)).iterator())));
        loadRetriever.checkAndCreateConsumer(0);

        Assertions.assertEquals(loadRetriever.buildBootstrapServer(), "192.168.0.1:9093,192.168.0.0:9092");
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegistrationChanged(new BrokerRegistrationChangeRecord().setBrokerId(0).setFenced(BrokerRegistrationFencingChange.FENCE.value()));
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegistrationChanged(new BrokerRegistrationChangeRecord().setBrokerId(1).setFenced(BrokerRegistrationFencingChange.FENCE.value()));
        Assertions.assertFalse(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertFalse(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegistrationChanged(new BrokerRegistrationChangeRecord().setBrokerId(1).setFenced(BrokerRegistrationFencingChange.UNFENCE.value()));
        Assertions.assertEquals(loadRetriever.buildBootstrapServer(), "192.168.0.1:9093");
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegister(new RegisterBrokerRecord().setBrokerId(1).setFenced(false).setEndPoints(
            new RegisterBrokerRecord.BrokerEndpointCollection(List.of(
                new RegisterBrokerRecord.BrokerEndpoint().setHost("192.168.0.2").setPort(9094)).iterator())));
        Assertions.assertFalse(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        Assertions.assertEquals(loadRetriever.buildBootstrapServer(), "192.168.0.2:9094");
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerUnregister(new UnregisterBrokerRecord().setBrokerId(1));
        Assertions.assertFalse(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertFalse(loadRetriever.hasAvailableBroker());
    }
}
