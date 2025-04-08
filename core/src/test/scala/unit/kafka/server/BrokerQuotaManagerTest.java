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

package unit.kafka.server;

import kafka.network.RequestChannel;
import kafka.server.QuotaType;
import kafka.server.ThrottleCallback;
import kafka.server.streamaspect.BrokerQuotaManager;
import kafka.utils.TestUtils;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.Session;
import org.apache.kafka.server.config.BrokerQuotaManagerConfig;
import org.apache.kafka.server.config.QuotaConfigs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("s3Unit")
public class BrokerQuotaManagerTest {
    private final Time time = Time.SYSTEM;

    private BrokerQuotaManager brokerQuotaManager;
    private final RequestChannel.Request request = mock(RequestChannel.Request.class);

    @BeforeEach
    public void setUp() {
        BrokerQuotaManagerConfig config = new BrokerQuotaManagerConfig(0, 3, 1);
        brokerQuotaManager = new BrokerQuotaManager(config, new Metrics(), time, "");

        Session session = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"), null);
        when(request.session()).thenReturn(session);

        RequestContext context = mock(RequestContext.class);
        when(context.clientId()).thenReturn("test");
        when(context.listenerName()).thenReturn("BROKER");
        when(request.context()).thenReturn(context);

        RequestHeader header = mock(RequestHeader.class);
        when(header.clientId()).thenReturn("test");
        when(request.header()).thenReturn(header);
    }

    @Test
    public void testQuota() {
        // Test produce quota
        Properties properties = new Properties();
        properties.put(QuotaConfigs.BROKER_QUOTA_ENABLED_CONFIG, true);
        properties.put(QuotaConfigs.BROKER_QUOTA_PRODUCE_BYTES_CONFIG, 100);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));

        long time = this.time.milliseconds();
        long second2millis = TimeUnit.SECONDS.toMillis(1);

        int result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 100, time);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 100, time + 10);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 100, time + second2millis);
        assertTrue(result > 0);

        properties.put(QuotaConfigs.BROKER_QUOTA_PRODUCE_BYTES_CONFIG, 1000);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 500, time + second2millis);
        assertEquals(0, result);

        // Test fetch quota
        properties.put(QuotaConfigs.BROKER_QUOTA_PRODUCE_BYTES_CONFIG, 0);
        properties.put(QuotaConfigs.BROKER_QUOTA_FETCH_BYTES_CONFIG, 100);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.fetch(), request, 100, time);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.fetch(), request, 100, time + 10);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.fetch(), request, 100, time + second2millis);
        assertTrue(result > 0);

        properties.put(QuotaConfigs.BROKER_QUOTA_FETCH_BYTES_CONFIG, 1000);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.fetch(), request, 500, time + second2millis);
        assertEquals(0, result);

        // Test slow fetch quota
        properties.put(QuotaConfigs.BROKER_QUOTA_FETCH_BYTES_CONFIG, 0);
        properties.put(QuotaConfigs.BROKER_QUOTA_SLOW_FETCH_BYTES_CONFIG, 100);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.slowFetch(), request, 100, time);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.slowFetch(), request, 100, time + 10);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.slowFetch(), request, 100, time + second2millis);
        assertTrue(result > 0);

        properties.put(QuotaConfigs.BROKER_QUOTA_SLOW_FETCH_BYTES_CONFIG, 1000);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.slowFetch(), request, 500, time + second2millis);
        assertEquals(0, result);

        // Test request quota
        properties.put(QuotaConfigs.BROKER_QUOTA_SLOW_FETCH_BYTES_CONFIG, 0);
        properties.put(QuotaConfigs.BROKER_QUOTA_REQUEST_RATE_CONFIG, 1);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 10);
        assertEquals(0, result);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + second2millis);
        assertTrue(result > 0);

        properties.put(QuotaConfigs.BROKER_QUOTA_REQUEST_RATE_CONFIG, 10);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 0, time + second2millis);
        assertEquals(0, result);
    }

    @Test
    public void testZeroQuota() {
        long result;
        long time = this.time.milliseconds();

        // enable quota
        Properties properties = new Properties();
        properties.put(QuotaConfigs.BROKER_QUOTA_ENABLED_CONFIG, true);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));

        brokerQuotaManager.updateQuota(QuotaType.requestRate(), 0);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time);
        assertEquals(1000, result);

        brokerQuotaManager.updateQuota(QuotaType.slowFetch(), 0);
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.slowFetch(), request, 1, time);
        assertEquals(1000, result);
    }

    @Test
    public void testUpdateQuota() {
        int result;
        long time = this.time.milliseconds();

        // enable quota
        Properties properties = new Properties();
        properties.put(QuotaConfigs.BROKER_QUOTA_ENABLED_CONFIG, true);
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));

        brokerQuotaManager.updateQuota(QuotaType.requestRate(), 1);
        // rate = 1 / 2000ms
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 1 / 2, time);
        assertEquals(0, result);
        // rate = 2 / 2010ms
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 10);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 2 / 2.01, time + 10);
        assertEquals(0, result);
        // rate = 3 / 2999ms > 1
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 2999);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 3 / 2.999, time + 2999);
        assertEquals(1, result);

        brokerQuotaManager.updateQuota(QuotaType.requestRate(), 2);
        // rate = 4 / 2999ms
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 2999);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 4 / 2.999, time + 2999);
        assertEquals(0, result);
        // rate = 5 / 2999ms
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 2999);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 5 / 2.999, time + 2999);
        assertEquals(0, result);
        // rate = 6 / 2999ms > 2
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 2999);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 6 / 2.999, time + 2999);
        assertEquals(1, result);

        brokerQuotaManager.updateQuota(QuotaType.requestRate(), 1);
        // rate = 5 / 2999ms > 1
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 2999 + 2999);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 5 / 2.999, time + 2999 + 2999);
        assertEquals(1000, result);
        // rate = 2 / 2001ms
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 2999 + 2999 + 1);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 2 / 2.001, time + 2999 + 2999 + 1);
        assertEquals(0, result);
        // rate = 3 / 2999ms > 1
        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.requestRate(), request, 1, time + 2999 + 2999 + 2999);
        assertQuotaMetricValue(QuotaType.requestRate(), (double) 3 / 2.999, time + 2999 + 2999 + 2999);
        assertEquals(1, result);
    }

    @Test
    public void testThrottle() {
        AtomicInteger throttleCounter = new AtomicInteger(0);
        brokerQuotaManager.throttle(QuotaType.requestRate(), new ThrottleCallback() {
            @Override
            public void startThrottling() {
                throttleCounter.incrementAndGet();
            }

            @Override
            public void endThrottling() {
                throttleCounter.incrementAndGet();
            }
        }, 100);
        assertEquals(1, throttleCounter.get());

        TestUtils.retry(1000, () -> {
            assertEquals(2, throttleCounter.get());
            return null;
        });
    }

    @Test
    public void testWhiteList() {
        // Test client id white list
        Properties properties = new Properties();
        properties.put(QuotaConfigs.BROKER_QUOTA_ENABLED_CONFIG, true);
        properties.put(QuotaConfigs.BROKER_QUOTA_PRODUCE_BYTES_CONFIG, 100);
        properties.put(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_CLIENT_ID_CONFIG, "test");
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));

        int result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 1000, time.milliseconds());
        assertEquals(0, result);

        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 1000, time.milliseconds());
        assertEquals(0, result);

        // Test remove white list
        properties.put(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_CLIENT_ID_CONFIG, "another_client_id");
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));

        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 1000, time.milliseconds());
        assertTrue(result > 0);

        // Test user white list
        properties.remove(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_CLIENT_ID_CONFIG);
        properties.remove(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_LISTENER_CONFIG);
        properties.put(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_USER_CONFIG, "user");
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));

        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 1000, time.milliseconds());
        assertEquals(0, result);

        // Test listener name white list
        properties.remove(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_USER_CONFIG);
        properties.remove(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_CLIENT_ID_CONFIG);
        properties.put(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_LISTENER_CONFIG, "BROKER");
        brokerQuotaManager.updateQuotaConfigs(Option.apply(properties));

        result = brokerQuotaManager.maybeRecordAndGetThrottleTimeMs(QuotaType.produce(), request, 1000, time.milliseconds());
        assertEquals(0, result);
    }

    private void assertQuotaMetricValue(QuotaType quotaType, double expected, long timeMs) {
        double value = brokerQuotaManager.getQuotaMetricValue(quotaType, timeMs).get();
        assertEquals(expected, value);
    }
}
