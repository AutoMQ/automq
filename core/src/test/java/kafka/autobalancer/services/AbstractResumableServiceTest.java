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
