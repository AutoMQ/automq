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
package org.apache.kafka.server.util;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InterBrokerAsyncSenderTest {

    private MockTime time;
    private KafkaClient networkClient;
    private InterBrokerAsyncSender sender;
    private Node node;
    private AbstractRequest.Builder<?> requestBuilder;
    private int requestTimeoutMs;

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        networkClient = mock(KafkaClient.class);
        requestTimeoutMs = 1000;
        sender = new InterBrokerAsyncSender("test-sender", networkClient, requestTimeoutMs, time);
        node = new Node(1, "localhost", 9092);
        requestBuilder = new StubRequestBuilder();
    }

    @Test
    public void testSendRequestSuccess() throws Exception {
        ClientRequest clientRequest = mock(ClientRequest.class);
        ClientResponse response = mock(ClientResponse.class);
        when(response.responseBody()).thenReturn(mock(AbstractResponse.class));
        
        ArgumentCaptor<RequestCompletionHandler> handlerCaptor = ArgumentCaptor.forClass(RequestCompletionHandler.class);
        
        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(requestBuilder),
            anyLong(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            handlerCaptor.capture()
        )).thenReturn(clientRequest);
        
        when(networkClient.ready(node, time.milliseconds())).thenReturn(true);
        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        CompletableFuture<ClientResponse> future = sender.sendRequest(node, requestBuilder);
        sender.pollOnce(100);

        // Simulate successful response
        handlerCaptor.getValue().onComplete(response);

        ClientResponse result = future.get(100, TimeUnit.MILLISECONDS);
        assertEquals(response, result);
        assertNotNull(result.responseBody());
    }

    @Test
    public void testSendRequestNodeNotReadyThenReady() throws Exception {
        ClientRequest clientRequest = mock(ClientRequest.class);
        ClientResponse response = mock(ClientResponse.class);
        when(response.responseBody()).thenReturn(mock(AbstractResponse.class));
        
        AbstractRequest.Builder<?> mockBuilder = mock(AbstractRequest.Builder.class);
        when(mockBuilder.latestAllowedVersion()).thenReturn((short) 1);
        doReturn(mockBuilder).when(clientRequest).requestBuilder();
        when(clientRequest.makeHeader(anyShort())).thenReturn(mock(RequestHeader.class));
        when(clientRequest.callback()).thenReturn(mock(RequestCompletionHandler.class));
        
        ArgumentCaptor<RequestCompletionHandler> handlerCaptor = ArgumentCaptor.forClass(RequestCompletionHandler.class);
        
        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(requestBuilder),
            anyLong(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            handlerCaptor.capture()
        )).thenReturn(clientRequest);
        
        // First call: node not ready
        when(networkClient.ready(node, time.milliseconds())).thenReturn(false).thenReturn(true);
        when(networkClient.connectionDelay(node, time.milliseconds())).thenReturn(0L);
        when(networkClient.connectionFailed(node)).thenReturn(false);
        when(networkClient.authenticationException(node)).thenReturn(null);
        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        CompletableFuture<ClientResponse> future = sender.sendRequest(node, requestBuilder);
        
        // First poll - node not ready
        sender.pollOnce(100);
        
        // Second poll - node ready
        sender.pollOnce(100);

        // Simulate successful response
        handlerCaptor.getValue().onComplete(response);

        ClientResponse result = future.get(100, TimeUnit.MILLISECONDS);
        assertEquals(response, result);
    }

    @Test
    public void testSendRequestConnectionFailed() throws Exception {
        ClientRequest clientRequest = mock(ClientRequest.class);
        
        AbstractRequest.Builder<?> mockBuilder = mock(AbstractRequest.Builder.class);
        when(mockBuilder.latestAllowedVersion()).thenReturn((short) 1);
        doReturn(mockBuilder).when(clientRequest).requestBuilder();
        when(clientRequest.makeHeader(anyShort())).thenReturn(mock(RequestHeader.class));
        
        ArgumentCaptor<RequestCompletionHandler> handlerCaptor = ArgumentCaptor.forClass(RequestCompletionHandler.class);
        
        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(requestBuilder),
            anyLong(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            handlerCaptor.capture()
        )).thenReturn(clientRequest);
        
        // Make the mock ClientRequest return the captured handler
        when(clientRequest.callback()).thenAnswer(invocation -> handlerCaptor.getValue());
        
        when(networkClient.ready(node, time.milliseconds())).thenReturn(false);
        when(networkClient.connectionDelay(node, time.milliseconds())).thenReturn(0L);
        when(networkClient.connectionFailed(node)).thenReturn(true);
        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        CompletableFuture<ClientResponse> future = sender.sendRequest(node, requestBuilder);
        sender.pollOnce(100);

        ExecutionException exception = assertThrows(ExecutionException.class, 
            () -> future.get(100, TimeUnit.MILLISECONDS));
        assertInstanceOf(DisconnectException.class, exception.getCause());
    }

    @Test
    public void testSendRequestAuthenticationFailed() throws Exception {
        ClientRequest clientRequest = mock(ClientRequest.class);
        AuthenticationException authException = new AuthenticationException("Auth failed");
        
        AbstractRequest.Builder<?> mockBuilder = mock(AbstractRequest.Builder.class);
        when(mockBuilder.latestAllowedVersion()).thenReturn((short) 1);
        doReturn(mockBuilder).when(clientRequest).requestBuilder();
        when(clientRequest.makeHeader(anyShort())).thenReturn(mock(RequestHeader.class));
        
        ArgumentCaptor<RequestCompletionHandler> handlerCaptor = ArgumentCaptor.forClass(RequestCompletionHandler.class);
        
        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(requestBuilder),
            anyLong(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            handlerCaptor.capture()
        )).thenReturn(clientRequest);
        
        // Make the mock ClientRequest return the captured handler
        when(clientRequest.callback()).thenAnswer(invocation -> handlerCaptor.getValue());
        
        when(networkClient.ready(node, time.milliseconds())).thenReturn(false);
        when(networkClient.connectionDelay(node, time.milliseconds())).thenReturn(0L);
        when(networkClient.connectionFailed(node)).thenReturn(true);
        when(networkClient.authenticationException(node)).thenReturn(authException);
        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        CompletableFuture<ClientResponse> future = sender.sendRequest(node, requestBuilder);
        sender.pollOnce(100);

        ExecutionException exception = assertThrows(ExecutionException.class, 
            () -> future.get(100, TimeUnit.MILLISECONDS));
        assertEquals(authException, exception.getCause());
    }

    @Test
    public void testSendRequestTimeout() throws Exception {
        ClientRequest clientRequest = mock(ClientRequest.class);
        
        AbstractRequest.Builder<?> mockBuilder = mock(AbstractRequest.Builder.class);
        when(mockBuilder.latestAllowedVersion()).thenReturn((short) 1);
        doReturn(mockBuilder).when(clientRequest).requestBuilder();
        when(clientRequest.makeHeader(anyShort())).thenReturn(mock(RequestHeader.class));
        when(clientRequest.createdTimeMs()).thenReturn(time.milliseconds());
        when(clientRequest.requestTimeoutMs()).thenReturn(requestTimeoutMs);
        
        ArgumentCaptor<RequestCompletionHandler> handlerCaptor = ArgumentCaptor.forClass(RequestCompletionHandler.class);
        
        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(requestBuilder),
            anyLong(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            handlerCaptor.capture()
        )).thenReturn(clientRequest);
        
        // Make the mock ClientRequest return the captured handler
        when(clientRequest.callback()).thenAnswer(invocation -> handlerCaptor.getValue());
        
        when(networkClient.ready(node, time.milliseconds())).thenReturn(false);
        when(networkClient.connectionDelay(node, time.milliseconds())).thenReturn(0L);
        when(networkClient.connectionFailed(node)).thenReturn(false);
        when(networkClient.authenticationException(node)).thenReturn(null);
        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        CompletableFuture<ClientResponse> future = sender.sendRequest(node, requestBuilder);
        
        // Advance time beyond timeout
        time.sleep(requestTimeoutMs + 1);
        sender.pollOnce(100);

        ExecutionException exception = assertThrows(ExecutionException.class, 
            () -> future.get(100, TimeUnit.MILLISECONDS));
        assertInstanceOf(DisconnectException.class, exception.getCause());
    }

    @Test
    public void testClose() throws Exception {
        sender.close();
        verify(networkClient).close();
    }

    @Test
    public void testWakeupCalledOnSendRequest() {
        CompletableFuture<ClientResponse> future = sender.sendRequest(node, requestBuilder);
        verify(networkClient).wakeup();
        assertTrue(future != null);
    }

    private static class StubRequestBuilder extends AbstractRequest.Builder<AbstractRequest> {
        public StubRequestBuilder() {
            super(ApiKeys.END_TXN);
        }

        @Override
        public AbstractRequest build(short version) {
            return mock(AbstractRequest.class);
        }
    }
}