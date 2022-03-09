/*
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
package org.apache.kafka.connect.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

@RunWith(PowerMockRunner.class)
public class RetryUtilTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private Callable<String> mockCallable;
    private final Supplier<String> testMsg = () -> "Test";

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        mockCallable = Mockito.mock(Callable.class);
    }

    @Test
    public void testSuccess() throws Exception {
        Mockito.when(mockCallable.call()).thenReturn("success");
        assertEquals("success", RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(100), 1));
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    // timeout the test after 1000ms if unable to complete within a reasonable time frame
    @Test(timeout = 1000)
    public void testExhaustingRetries() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException());
        ConnectException e = assertThrows(ConnectException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(50), 10));
        Mockito.verify(mockCallable, Mockito.atLeastOnce()).call();
    }

    @Test
    public void retriesEventuallySucceed() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenReturn("success");

        assertEquals("success", RetryUtil.retryUntilTimeout(mockCallable, testMsg, TIMEOUT, 1));
        Mockito.verify(mockCallable, Mockito.times(4)).call();
    }

    @Test
    public void failWithNonRetriableException() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new NullPointerException("Non retriable"));
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, testMsg, TIMEOUT, 0));
        assertEquals("Non retriable", e.getMessage());
        Mockito.verify(mockCallable, Mockito.times(6)).call();
    }

    @Test
    public void noRetryAndSucceed() throws Exception {
        Mockito.when(mockCallable.call()).thenReturn("success");

        assertEquals("success", RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(0), 100));
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test
    public void noRetryAndFailed() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException("timeout exception"));

        TimeoutException e = assertThrows(TimeoutException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(0), 100));
        Mockito.verify(mockCallable, Mockito.times(1)).call();
        assertEquals("timeout exception", e.getMessage());
    }

    @Test
    public void testNoBackoffTimeAndSucceed() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenReturn("success");

        assertEquals("success", RetryUtil.retryUntilTimeout(mockCallable, testMsg, TIMEOUT, 0));
        Mockito.verify(mockCallable, Mockito.times(4)).call();
    }

    @Test
    public void testNoBackoffTimeAndFail() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException("timeout exception"));

        ConnectException e = assertThrows(ConnectException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(80), 0));
        Mockito.verify(mockCallable, Mockito.atLeastOnce()).call();
        assertTrue(e.getMessage().contains("Reason: timeout exception"));
    }

    @Test
    public void testBackoffMoreThanTimeoutWillOnlyExecuteOnce() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException("timeout exception"));

        TimeoutException e = assertThrows(TimeoutException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(50), 100));
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test
    public void testInvalidTimeDuration() throws Exception {
        Mockito.when(mockCallable.call()).thenReturn("success");

        assertEquals("success", RetryUtil.retryUntilTimeout(mockCallable, testMsg, null, 10));
        assertEquals("success", RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(-1), 10));
        Mockito.verify(mockCallable, Mockito.times(2)).call();
    }

    @Test
    public void testInvalidRetryTimeout() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException("timeout"))
                .thenReturn("success");
        assertEquals("success", RetryUtil.retryUntilTimeout(mockCallable, testMsg, TIMEOUT, -1));
        Mockito.verify(mockCallable, Mockito.times(2)).call();
    }

    @Test
    public void testSupplier() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException("timeout exception"));

        ConnectException e = assertThrows(ConnectException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, null, Duration.ofMillis(100), 10));
        assertTrue(e.getMessage().startsWith("Fail to callable"));

        e = assertThrows(ConnectException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, () -> null, Duration.ofMillis(100), 10));
        assertTrue(e.getMessage().startsWith("Fail to callable"));

        e = assertThrows(ConnectException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, () -> "execute lambda", Duration.ofMillis(500), 10));
        assertTrue(e.getMessage().startsWith("Fail to execute lambda"));
        Mockito.verify(mockCallable, Mockito.atLeast(3)).call();
    }

    @Test
    public void testWakeupException() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new WakeupException());

        ConnectException e = assertThrows(ConnectException.class,
                () -> RetryUtil.retryUntilTimeout(mockCallable, testMsg, Duration.ofMillis(50), 10));
        Mockito.verify(mockCallable, Mockito.atLeastOnce()).call();
    }
}
