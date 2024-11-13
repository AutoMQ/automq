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
