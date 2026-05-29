/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.network;

import kafka.automq.retrystorm.RetryStormBackoffConfig;
import kafka.automq.retrystorm.RetryStormBackoffStateStore;
import kafka.server.retrystorm.RetryStormBackoffPolicy;
import kafka.server.retrystorm.RetryStormDelayedResponseScheduler;
import kafka.server.retrystorm.RetryStormResponseGate;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers retry storm response gating at the RequestChannel send boundary.
 */
@Tag("S3Unit")
public class RequestChannelRetryStormTest {

    /**
     * Given RequestChannel owns retry storm gating, the second all-error response is scheduled before enqueue.
     */
    @Test
    public void testRequestChannelSchedulesRetryStormDelay() throws Exception {
        CapturingScheduler scheduler = new CapturingScheduler();
        RetryStormResponseGate gate = new RetryStormResponseGate(
            new RetryStormBackoffPolicy(
                new RetryStormBackoffConfig(true, 1000L),
                new RetryStormBackoffStateStore()
            ),
            scheduler
        );
        RequestChannel channel = new RequestChannel(
            10,
            "",
            Time.SYSTEM,
            new RequestChannel.Metrics(scala.jdk.javaapi.CollectionConverters.asScala(List.of(ApiKeys.PRODUCE)))
        );
        channel.setRetryStormResponseGate(gate);

        channel.sendResponse(request(), allErrorProduceResponse(), scala.Option.empty());
        channel.sendResponse(request(), allErrorProduceResponse(), scala.Option.empty());

        assertEquals(1, scheduler.scheduled.get());
    }

    private static RequestChannel.Request request() throws Exception {
        ProduceRequest request = new ProduceRequest.Builder(
            ApiKeys.PRODUCE.latestVersion(),
            ApiKeys.PRODUCE.latestVersion(),
            new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000)
        ).build();
        return request(request);
    }

    private static ProduceResponse allErrorProduceResponse() {
        ProduceResponseData responseData = new ProduceResponseData();
        responseData.responses().add(new ProduceResponseData.TopicProduceResponse().setName("topic").setPartitionResponses(List.of(
            new ProduceResponseData.PartitionProduceResponse().setIndex(0).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
        )));
        return new ProduceResponse(responseData);
    }

    private static RequestChannel.Request request(AbstractRequest request) throws Exception {
        ByteBuffer buffer = request.serializeWithHeader(new RequestHeader(request.apiKey(), request.version(), "client-id", 1));
        RequestHeader header = RequestHeader.parse(buffer);
        RequestContext context = new RequestContext(
            header,
            "connection-id",
            InetAddress.getLoopbackAddress(),
            Optional.empty(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false,
            Optional.empty()
        );
        return new RequestChannel.Request(
            1,
            context,
            0L,
            MemoryPool.NONE,
            buffer,
            new RequestChannel.Metrics(scala.jdk.javaapi.CollectionConverters.asScala(List.of(ApiKeys.PRODUCE))),
            scala.Option.apply(null)
        );
    }

    private static class CapturingScheduler extends RetryStormDelayedResponseScheduler {
        private final AtomicInteger scheduled = new AtomicInteger(0);

        private CapturingScheduler() {
            super(10L);
        }

        @Override
        public void schedule(long delayMs, Runnable sendNow) {
            scheduled.incrementAndGet();
        }
    }
}
