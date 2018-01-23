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
package org.apache.kafka.common.network;

import java.nio.channels.SelectionKey;
import javax.net.ssl.SSLEngine;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.memory.SimpleMemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SslSelectorTest extends SelectorTest {

    private Map<String, Object> sslClientConfigs;

    @Before
    public void setUp() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");

        Map<String, Object> sslServerConfigs = TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        this.server = new EchoServer(SecurityProtocol.SSL, sslServerConfigs);
        this.server.start();
        this.time = new MockTime();
        sslClientConfigs = TestSslUtils.createSslConfig(false, false, Mode.CLIENT, trustStoreFile, "client");
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT, null, false);
        this.channelBuilder.configure(sslClientConfigs);
        this.metrics = new Metrics();
        this.selector = new Selector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext());
    }

    @After
    public void tearDown() throws Exception {
        this.selector.close();
        this.server.close();
        this.metrics.close();
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    @Test
    public void testDisconnectWithIntermediateBufferedBytes() throws Exception {
        int requestSize = 100 * 1024;
        final String node = "0";
        String request = TestUtils.randomString(requestSize);

        this.selector.close();

        this.channelBuilder = new TestSslChannelBuilder(Mode.CLIENT);
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, metrics, time, "MetricGroup", channelBuilder, new LogContext());
        connect(node, new InetSocketAddress("localhost", server.port));
        selector.send(createSend(node, request));

        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                try {
                    selector.poll(0L);
                    return selector.channel(node).hasBytesBuffered();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 2000L, "Failed to reach socket state with bytes buffered");

        selector.close(node);
        verifySelectorEmpty();
    }

    /**
     * Renegotiation is not supported since it is potentially unsafe and it has been removed in TLS 1.3
     */
    @Test
    public void testRenegotiationFails() throws Exception {
        String node = "0";
        // create connections
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        // send echo requests and receive responses
        while (!selector.isChannelReady(node)) {
            selector.poll(1000L);
        }
        selector.send(createSend(node, node + "-" + 0));
        selector.poll(0L);
        server.renegotiate();
        selector.send(createSend(node, node + "-" + 1));
        long expiryTime = System.currentTimeMillis() + 2000;

        List<String> disconnected = new ArrayList<>();
        while (!disconnected.contains(node) && System.currentTimeMillis() < expiryTime) {
            selector.poll(10);
            disconnected.addAll(selector.disconnected().keySet());
        }
        assertTrue("Renegotiation should cause disconnection", disconnected.contains(node));

    }

    @Override
    public void testMuteOnOOM() throws Exception {
        //clean up default selector, replace it with one that uses a finite mem pool
        selector.close();
        MemoryPool pool = new SimpleMemoryPool(900, 900, false, null);
        //the initial channel builder is for clients, we need a server one
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslServerConfigs = TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        channelBuilder = new SslChannelBuilder(Mode.SERVER, null, false);
        channelBuilder.configure(sslServerConfigs);
        selector = new Selector(NetworkReceive.UNLIMITED, 5000, metrics, time, "MetricGroup",
                new HashMap<String, String>(), true, false, channelBuilder, pool, new LogContext());

        try (ServerSocketChannel ss = ServerSocketChannel.open()) {
            ss.bind(new InetSocketAddress(0));

            InetSocketAddress serverAddress = (InetSocketAddress) ss.getLocalAddress();

            SslSender sender1 = createSender(serverAddress, randomPayload(900));
            SslSender sender2 = createSender(serverAddress, randomPayload(900));
            sender1.start();
            sender2.start();

            SocketChannel channelX = ss.accept(); //not defined if its 1 or 2
            channelX.configureBlocking(false);
            SocketChannel channelY = ss.accept();
            channelY.configureBlocking(false);
            selector.register("clientX", channelX);
            selector.register("clientY", channelY);

            boolean handshaked = false;
            NetworkReceive firstReceive = null;
            long deadline = System.currentTimeMillis() + 5000;
            //keep calling poll until:
            //1. both senders have completed the handshakes (so server selector has tried reading both payloads)
            //2. a single payload is actually read out completely (the other is too big to fit)
            while (System.currentTimeMillis() < deadline) {
                selector.poll(10);

                List<NetworkReceive> completed = selector.completedReceives();
                if (firstReceive == null) {
                    if (!completed.isEmpty()) {
                        assertEquals("expecting a single request", 1, completed.size());
                        firstReceive = completed.get(0);
                        assertTrue(selector.isMadeReadProgressLastPoll());
                        assertEquals(0, pool.availableMemory());
                    }
                } else {
                    assertTrue("only expecting single request", completed.isEmpty());
                }

                handshaked = sender1.waitForHandshake(1) && sender2.waitForHandshake(1);

                if (handshaked && firstReceive != null && selector.isOutOfMemory())
                    break;
            }
            assertTrue("could not initiate connections within timeout", handshaked);

            selector.poll(10);
            assertTrue(selector.completedReceives().isEmpty());
            assertEquals(0, pool.availableMemory());
            assertNotNull("First receive not complete", firstReceive);
            assertTrue("Selector not out of memory", selector.isOutOfMemory());

            firstReceive.close();
            assertEquals(900, pool.availableMemory()); //memory has been released back to pool

            List<NetworkReceive> completed = Collections.emptyList();
            deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && completed.isEmpty()) {
                selector.poll(1000);
                completed = selector.completedReceives();
            }
            assertEquals("could not read remaining request within timeout", 1, completed.size());
            assertEquals(0, pool.availableMemory());
            assertFalse(selector.isOutOfMemory());
        }
    }

    /**
     * Connects and waits for handshake to complete. This is required since SslTransportLayer
     * implementation requires the channel to be ready before send is invoked (unlike plaintext
     * where send can be invoked straight after connect)
     */
    protected void connect(String node, InetSocketAddress serverAddr) throws IOException {
        blockingConnect(node, serverAddr);
    }

    private SslSender createSender(InetSocketAddress serverAddress, byte[] payload) {
        return new SslSender(serverAddress, payload);
    }

    private static class TestSslChannelBuilder extends SslChannelBuilder {

        public TestSslChannelBuilder(Mode mode) {
            super(mode, null, false);
        }

        @Override
        protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key, String host) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            SSLEngine sslEngine = sslFactory.createSslEngine(host, socketChannel.socket().getPort());
            TestSslTransportLayer transportLayer = new TestSslTransportLayer(id, key, sslEngine);
            transportLayer.startHandshake();
            return transportLayer;
        }

        /*
         * TestSslTransportLayer will read from socket once every two tries. This increases
         * the chance that there will be bytes buffered in the transport layer after read().
         */
        class TestSslTransportLayer extends SslTransportLayer {
            boolean muteSocket = false;

            public TestSslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine) throws IOException {
                super(channelId, key, sslEngine);
            }

            @Override
            protected int readFromSocketChannel() throws IOException {
                if (muteSocket) {
                    muteSocket = false;
                    return 0;
                }
                muteSocket = true;
                return super.readFromSocketChannel();
            }
        }
    }

}
