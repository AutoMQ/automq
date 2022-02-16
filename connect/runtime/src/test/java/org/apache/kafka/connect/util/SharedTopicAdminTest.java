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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Rule;
import org.mockito.Mock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.apache.kafka.connect.util.SharedTopicAdmin.DEFAULT_CLOSE_DURATION;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SharedTopicAdminTest {

    private static final Map<String, Object> EMPTY_CONFIG = Collections.emptyMap();

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock private TopicAdmin mockTopicAdmin;
    @Mock private Function<Map<String, Object>, TopicAdmin> factory;
    private SharedTopicAdmin sharedAdmin;

    @Before
    public void beforeEach() {
        when(factory.apply(anyMap())).thenReturn(mockTopicAdmin);
        sharedAdmin = new SharedTopicAdmin(EMPTY_CONFIG, factory);
    }

    @Test
    public void shouldCloseWithoutBeingUsed() {
        // When closed before being used
        sharedAdmin.close();
        // Then should not create or close admin
        verifyTopicAdminCreatesAndCloses(0);
    }

    @Test
    public void shouldCloseAfterTopicAdminUsed() {
        // When used and then closed
        assertSame(mockTopicAdmin, sharedAdmin.topicAdmin());
        sharedAdmin.close();
        // Then should have created and closed just one admin
        verifyTopicAdminCreatesAndCloses(1);
    }

    @Test
    public void shouldCloseAfterTopicAdminUsedMultipleTimes() {
        // When used many times and then closed
        for (int i = 0; i != 10; ++i) {
            assertSame(mockTopicAdmin, sharedAdmin.topicAdmin());
        }
        sharedAdmin.close();
        // Then should have created and closed just one admin
        verifyTopicAdminCreatesAndCloses(1);
    }

    @Test
    public void shouldCloseWithDurationAfterTopicAdminUsed() {
        // When used and then closed with a custom timeout
        Duration timeout = Duration.ofSeconds(1);
        assertSame(mockTopicAdmin, sharedAdmin.topicAdmin());
        sharedAdmin.close(timeout);
        // Then should have created and closed just one admin using the supplied timeout
        verifyTopicAdminCreatesAndCloses(1, timeout);
    }

    @Test
    public void shouldFailToGetTopicAdminAfterClose() {
        // When closed
        sharedAdmin.close();
        // Then using the admin should fail
        assertThrows(ConnectException.class, () -> sharedAdmin.topicAdmin());
    }

    private void verifyTopicAdminCreatesAndCloses(int count) {
        verifyTopicAdminCreatesAndCloses(count, DEFAULT_CLOSE_DURATION);
    }

    private void verifyTopicAdminCreatesAndCloses(int count, Duration expectedDuration) {
        verify(factory, times(count)).apply(anyMap());
        verify(mockTopicAdmin, times(count)).close(eq(expectedDuration));
    }
}