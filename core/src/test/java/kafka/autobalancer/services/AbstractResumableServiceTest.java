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

package kafka.autobalancer.services;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AbstractResumableServiceTest {

    @Test
    public void testServiceOperations() {
        AbstractResumableService service = createService();
        service.pause();
        Mockito.verify(service, Mockito.times(0)).doPause();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertFalse(service.isShutdown());
        Assertions.assertEquals(0, service.currentEpoch());
        service.shutdown();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertTrue(service.isShutdown());
        Assertions.assertEquals(0, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doShutdown();
        service.run();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertTrue(service.isShutdown());
        Assertions.assertEquals(0, service.currentEpoch());
        Mockito.verify(service, Mockito.times(0)).doRun();

        service = createService();
        service.run();
        Assertions.assertTrue(service.isRunning());
        Assertions.assertFalse(service.isShutdown());
        Assertions.assertEquals(1, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doRun();
        service.pause();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertFalse(service.isShutdown());
        Assertions.assertEquals(1, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doPause();
        service.pause();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertFalse(service.isShutdown());
        Assertions.assertEquals(1, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doPause();
        service.run();
        Assertions.assertTrue(service.isRunning());
        Assertions.assertFalse(service.isShutdown());
        Assertions.assertEquals(2, service.currentEpoch());
        Mockito.verify(service, Mockito.times(2)).doRun();
        service.shutdown();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertTrue(service.isShutdown());
        Assertions.assertEquals(2, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doShutdown();
        service.pause();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertTrue(service.isShutdown());
        Assertions.assertEquals(2, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doPause();

        service = createService();
        service.run();
        Assertions.assertTrue(service.isRunning());
        Assertions.assertFalse(service.isShutdown());
        Assertions.assertEquals(1, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doRun();
        service.pause();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertFalse(service.isShutdown());
        Assertions.assertEquals(1, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doPause();
        service.shutdown();
        Assertions.assertFalse(service.isRunning());
        Assertions.assertTrue(service.isShutdown());
        Assertions.assertEquals(1, service.currentEpoch());
        Mockito.verify(service, Mockito.times(1)).doShutdown();
    }

    public AbstractResumableService createService() {
        AbstractResumableService service = new AbstractResumableService(null) {
            @Override
            protected void doRun() {

            }

            @Override
            protected void doShutdown() {

            }

            @Override
            protected void doPause() {

            }
        };
        return Mockito.spy(service);
    }
}
