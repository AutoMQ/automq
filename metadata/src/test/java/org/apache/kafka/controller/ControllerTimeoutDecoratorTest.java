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

package org.apache.kafka.controller;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ControllerTimeoutDecoratorTest {

    @Test
    public void testLowPriorityWithTimeout() {
        var controllerMock = mock(Controller.class);
        when(controllerMock.checkS3ObjectsLifecycle(any()))
                .then(invocation -> CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));
        var timeoutDecorator = new ControllerTimeoutDecorator(controllerMock, 1);

        CompletableFuture<Void> result = timeoutDecorator.checkS3ObjectsLifecycle(null);

        Throwable originalTimeoutException = assertThrowsExactly(ExecutionException.class, result::get).getCause();
        assertEquals(TimeoutException.class, originalTimeoutException.getClass());
    }

    @Test
    public void testLowPriorityWithoutTimeout() {
        var controllerMock = mock(Controller.class);
        when(controllerMock.checkS3ObjectsLifecycle(any()))
                .then(invocation -> CompletableFuture.runAsync(() -> { }));
        var timeoutDecorator = new ControllerTimeoutDecorator(controllerMock, 100);

        CompletableFuture<Void> result = timeoutDecorator.checkS3ObjectsLifecycle(null);

        assertDoesNotThrow(() -> result.get());
    }

}
