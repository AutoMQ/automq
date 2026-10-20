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

package com.automq.stream.s3.operator;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class S3ConnectionFailoverTest {
    private static final InetAddress REFUSED = address(2);
    private static final InetAddress HEALTHY = address(1);
    private static final InetAddress NEW_ADDRESS = address(3);
    private static final InetSocketAddress ENDPOINT = InetSocketAddress.createUnresolved("localhost", 9000);
    private static final String TLS_HOSTNAME = "s3.failover.test";
    private static final String KEYSTORE_PASSWORD = "local-test-keystore";
    private static final AwsObjectStorage.Configuration CONFIGURATION = AwsObjectStorage.Configuration.builder()
        .maxConcurrency(2)
        .apiCallTimeout(Duration.ofSeconds(10))
        .apiCallAttemptTimeout(Duration.ofSeconds(3))
        .build();

    /** Given a refused address, an HTTPS retry preserves SNI and validates the certificate for the original DNS name. */
    @Test
    void httpsRetryPreservesOriginalHostnameForSniAndCertificateVerification(@TempDir Path directory) throws Exception {
        runHttpsRegression(directory, TLS_HOSTNAME, true);
    }

    /** A trusted certificate naming a different DNS host must reject the HTTPS retry before any upload reaches the server. */
    @Test
    void httpsRetryRejectsTrustedCertificateForAnotherHostname(@TempDir Path directory) throws Exception {
        runHttpsRegression(directory, "another.failover.test", false);
    }

    private static void runHttpsRegression(Path directory, String certificateHostname, boolean matchingHostname) throws Exception {
        Path keyStoreFile = directory.resolve("server.p12");
        Path certificateOutput = directory.resolve("keytool.log");
        Process keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
            "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-validity", "1",
            "-dname", "CN=" + certificateHostname, "-ext", "SAN=dns:" + certificateHostname,
            "-storetype", "PKCS12", "-keystore", keyStoreFile.toString(), "-storepass", KEYSTORE_PASSWORD,
            "-keypass", KEYSTORE_PASSWORD, "-noprompt")
            .redirectErrorStream(true).redirectOutput(certificateOutput.toFile()).start();
        try {
            assertTrue(keytool.waitFor(20, TimeUnit.SECONDS), "Generating the local TLS certificate must finish");
            assertEquals(0, keytool.exitValue(), Files.readString(certificateOutput));
        } finally {
            keytool.destroyForcibly();
        }

        Path hostsFile = directory.resolve("hosts");
        Files.writeString(hostsFile, "127.0.0.1 " + TLS_HOSTNAME + System.lineSeparator());
        // InetAddress initializes its hosts-file resolver once. A fresh JVM also isolates the test trust store.
        List<String> classPath = new ArrayList<>();
        classPath.add(System.getProperty("java.class.path"));
        for (ClassLoader loader = S3ConnectionFailoverTest.class.getClassLoader(); loader != null; loader = loader.getParent()) {
            if (loader instanceof URLClassLoader) {
                for (URL url : ((URLClassLoader) loader).getURLs()) {
                    classPath.add(Path.of(url.toURI()).toString());
                }
            }
        }
        Path output = directory.resolve("https-regression.log");
        Process regression = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djdk.net.hosts.file=" + hostsFile, "-Djavax.net.ssl.trustStore=" + keyStoreFile,
            "-Djavax.net.ssl.trustStoreType=PKCS12", "-Djavax.net.ssl.trustStorePassword=" + KEYSTORE_PASSWORD,
            "-cp", String.join(File.pathSeparator, classPath), HttpsRegression.class.getName(), keyStoreFile.toString(),
            Boolean.toString(matchingHostname))
            .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            if (!regression.waitFor(30, TimeUnit.SECONDS)) {
                Path threadDump = directory.resolve("https-threads.log");
                Process diagnostics = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
                    Long.toString(regression.pid()), "Thread.print")
                    .redirectErrorStream(true).redirectOutput(threadDump.toFile()).start();
                try {
                    diagnostics.waitFor(5, TimeUnit.SECONDS);
                } finally {
                    diagnostics.destroyForcibly();
                }
                throw new AssertionError("The isolated HTTPS regression must finish. Child output:\n"
                    + Files.readString(output) + "\nThread dump:\n" + Files.readString(threadDump));
            }
            assertEquals(0, regression.exitValue(), Files.readString(output));
        } finally {
            regression.destroyForcibly();
        }
    }

    /** Isolates the local DNS mapping and TLS trust store from the rest of the test JVM. */
    public static class HttpsRegression {
        /** Runs the HTTPS retry scenario using the supplied keystore and whether its certificate matches the request host. */
        public static void main(String[] args) throws Exception {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
                keyStore.load(input, KEYSTORE_PASSWORD.toCharArray());
            }
            KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(keyStore, KEYSTORE_PASSWORD.toCharArray());
            SSLContext serverContext = SSLContext.getInstance("TLS");
            serverContext.init(keyManagers.getKeyManagers(), null, null);
            AtomicReference<String> sni = new AtomicReference<>();
            AtomicReference<String> host = new AtomicReference<>();
            AtomicReference<byte[]> body = new AtomicReference<>();
            AtomicInteger requests = new AtomicInteger();
            HttpsServer server = HttpsServer.create(new InetSocketAddress(HEALTHY, 0), 0);
            server.setHttpsConfigurator(new HttpsConfigurator(serverContext) {
                @Override
                public void configure(HttpsParameters parameters) {
                    SSLParameters sslParameters = serverContext.getDefaultSSLParameters();
                    sslParameters.setSNIMatchers(List.of(new SNIMatcher(0) {
                        @Override
                        public boolean matches(SNIServerName serverName) {
                            sni.set(((SNIHostName) serverName).getAsciiName());
                            return true;
                        }
                    }));
                    parameters.setSSLParameters(sslParameters);
                }
            });
            server.createContext("/bucket/object", exchange -> {
                host.set(exchange.getRequestHeaders().getFirst("Host"));
                body.set(exchange.getRequestBody().readAllBytes());
                requests.incrementAndGet();
                exchange.getResponseHeaders().set("ETag", "\"etag\"");
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            });
            AtomicInteger resolutions = new AtomicInteger();
            S3ConnectionFailover failover = new S3ConnectionFailover(hostname -> {
                assertEquals(TLS_HOSTNAME, hostname);
                resolutions.incrementAndGet();
                return new InetAddress[] {REFUSED, HEALTHY};
            }, System::nanoTime);
            AwsObjectStorage storage = storage(failover);
            int port = server.getAddress().getPort();
            byte[] payload = "TLS retry preserves this object".getBytes(StandardCharsets.UTF_8);
            server.start();
            try (S3AsyncClient client = storage.newS3Client(
                BucketURI.parse("0@s3://bucket?region=us-east-1&endpoint=https://" + TLS_HOSTNAME + ":" + port + "&pathStyle=true"),
                List.of(AnonymousCredentialsProvider.create()), CONFIGURATION)) {
                CompletableFuture<?> upload = client.putObject(PutObjectRequest.builder().bucket("bucket").key("object").build(),
                    AsyncRequestBody.fromBytes(payload));
                if (Boolean.parseBoolean(args[1])) {
                    upload.get(10, TimeUnit.SECONDS);
                    assertEquals(TLS_HOSTNAME + ":" + port, host.get());
                    assertArrayEquals(payload, body.get());
                    assertEquals(1, requests.get());
                } else {
                    ExecutionException failure = assertThrows(ExecutionException.class, () -> upload.get(10, TimeUnit.SECONDS));
                    boolean hostnameRejected = false;
                    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                        if (cause instanceof CertificateException && cause.getMessage() != null
                            && cause.getMessage().contains(TLS_HOSTNAME)) {
                            hostnameRejected = true;
                        }
                    }
                    assertTrue(hostnameRejected, "A trusted certificate with the wrong DNS name must fail hostname verification: " + failure);
                    assertEquals(0, requests.get(), "TLS hostname rejection must prevent the HTTP upload");
                }
                assertTrue(resolutions.get() >= 2, "The HTTPS attempt must retry after an actual TCP refusal");
                assertEquals(HEALTHY, failover.select(InetSocketAddress.createUnresolved(TLS_HOSTNAME, port)).getAddress());
                assertEquals(TLS_HOSTNAME, sni.get(), "TLS SNI must retain the original hostname after changing IP addresses");
            } finally {
                server.stop(0);
                storage.close();
            }
        }
    }

    /**
     * Given a real refused TCP address followed by a live server, the production S3 client construction
     * must wire failover into SDK retries, preserve the original Host and body, and close its owned loops.
     */
    @Test
    void sdkRetrySwitchesAddressAndClosesEventLoops() throws Exception {
        Set<Long> previousThreads = nettyThreads().stream().map(Thread::getId).collect(Collectors.toSet());
        HttpServer server = HttpServer.create(new InetSocketAddress(HEALTHY, 0), 0);
        AtomicReference<String> host = new AtomicReference<>();
        AtomicReference<byte[]> body = new AtomicReference<>();
        AtomicInteger requests = new AtomicInteger();
        server.createContext("/bucket/object", exchange -> {
            host.set(exchange.getRequestHeaders().getFirst("Host"));
            body.set(exchange.getRequestBody().readAllBytes());
            requests.incrementAndGet();
            exchange.getResponseHeaders().set("ETag", "\"etag\"");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        AtomicInteger resolutions = new AtomicInteger();
        S3ConnectionFailover failover = new S3ConnectionFailover(hostname -> {
            assertEquals("localhost", hostname);
            resolutions.incrementAndGet();
            return new InetAddress[] {REFUSED, HEALTHY};
        }, System::nanoTime);
        AwsObjectStorage storage = storage(failover);
        int port = server.getAddress().getPort();
        byte[] payload = "retry must preserve this object".getBytes(StandardCharsets.UTF_8);
        List<Thread> ownedThreads = new ArrayList<>();
        try (S3AsyncClient client = storage.newS3Client(
            BucketURI.parse("0@s3://bucket?region=us-east-1&endpoint=http://localhost:" + port + "&pathStyle=true"),
            List.of(AnonymousCredentialsProvider.create()), CONFIGURATION)) {
            client.putObject(PutObjectRequest.builder().bucket("bucket").key("object").build(),
                AsyncRequestBody.fromBytes(payload)).get(10, TimeUnit.SECONDS);
            assertTrue(resolutions.get() >= 2, "The SDK must retry after the actual first TCP refusal");
            assertEquals(HEALTHY, failover.select(InetSocketAddress.createUnresolved("localhost", port)).getAddress());
            assertEquals("localhost:" + port, host.get());
            assertArrayEquals(payload, body.get());
            assertEquals(1, requests.get());
            ownedThreads.addAll(nettyThreads().stream()
                .filter(thread -> !previousThreads.contains(thread.getId())).collect(Collectors.toList()));
            assertFalse(ownedThreads.isEmpty(), "The production factory must create an SDK-owned event loop");
            assertTrue(ownedThreads.size() <= 2, "Per-client event loop thread count must remain bounded");
        } finally {
            server.stop(0);
            storage.close();
        }
        for (Thread thread : ownedThreads) {
            thread.join(10000);
            assertFalse(thread.isAlive(), "Closing the S3 client must release its event loop: " + thread.getName());
        }
    }

    /** A preattached SDK listener must see the failed IP quarantined before it immediately starts a retry. */
    @Test
    void refusalIsRecordedBeforeExistingConnectListenersRun() throws Exception {
        S3ConnectionFailover failover = policy(new AtomicLong());
        NioEventLoopGroup eventLoops = new NioEventLoopGroup(1);
        SocketChannel channel = failover.newChannel();
        try (ServerSocket server = new ServerSocket(0, 50, HEALTHY)) {
            eventLoops.register(channel).sync();
            InetSocketAddress endpoint = InetSocketAddress.createUnresolved("localhost", server.getLocalPort());
            CompletableFuture<InetAddress> nextAddress = new CompletableFuture<>();
            ChannelPromise originalPromise = channel.newPromise();
            originalPromise.addListener(future -> {
                try {
                    if (future.isSuccess()) {
                        nextAddress.completeExceptionally(new AssertionError("The first TCP connection must be refused"));
                    } else {
                        nextAddress.complete(failover.select(endpoint).getAddress());
                    }
                } catch (Throwable failure) {
                    nextAddress.completeExceptionally(failure);
                }
            });
            channel.connect(endpoint, null, originalPromise);
            assertEquals(HEALTHY, nextAddress.get(10, TimeUnit.SECONDS));
            assertTrue(originalPromise.cause() instanceof ConnectException);
        } finally {
            channel.close().syncUninterruptibly();
            eventLoops.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /** Cancelling the caller's promise must terminate the channel instead of leaving an unowned connection. */
    @Test
    void cancelledConnectClosesTheChannel() throws Exception {
        CountDownLatch resolving = new CountDownLatch(1);
        CountDownLatch continueConnect = new CountDownLatch(1);
        S3ConnectionFailover failover = new S3ConnectionFailover(hostname -> {
            resolving.countDown();
            try {
                if (!continueConnect.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("The caller must cancel while connect is in progress");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            return new InetAddress[] {REFUSED, HEALTHY};
        }, System::nanoTime);
        NioEventLoopGroup eventLoops = new NioEventLoopGroup(1);
        SocketChannel channel = failover.newChannel();
        try {
            eventLoops.register(channel).sync();
            ChannelPromise originalPromise = channel.newPromise();
            channel.connect(ENDPOINT, null, originalPromise);
            assertTrue(resolving.await(10, TimeUnit.SECONDS));
            assertTrue(originalPromise.cancel(false));
            continueConnect.countDown();
            assertTrue(channel.closeFuture().await(10, TimeUnit.SECONDS));
            assertTrue(originalPromise.isCancelled());
        } finally {
            continueConnect.countDown();
            channel.close().syncUninterruptibly();
            eventLoops.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /** Given a refusal on one channel, concurrent later connections must all avoid that address. */
    @Test
    void refusalIsVisibleAcrossConcurrentConnections() throws Exception {
        S3ConnectionFailover failover = policy(new AtomicLong());
        InetSocketAddress first = failover.select(ENDPOINT);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            executor.submit(() -> failover.connectionFailed(first,
                new IOException("connection failed", new ConnectException("Connection refused")))).get();
            List<CompletableFuture<InetAddress>> connections = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                connections.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        return failover.select(ENDPOINT).getAddress();
                    } catch (UnknownHostException e) {
                        throw new AssertionError(e);
                    }
                }, executor));
            }
            for (CompletableFuture<InetAddress> connection : connections) {
                assertEquals(HEALTHY, connection.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** Given all addresses refused, retry the oldest failure and allow the normal address after cooldown. */
    @Test
    void allFailedAddressesRemainRetryableAndCooldownExpires() throws Exception {
        AtomicLong now = new AtomicLong(-100);
        S3ConnectionFailover failover = policy(now);
        failover.connectionFailed(failover.select(ENDPOINT), new ConnectException("Connection refused"));
        now.incrementAndGet();
        failover.connectionFailed(failover.select(ENDPOINT), new ConnectException("Connection refused"));
        assertEquals(REFUSED, failover.select(ENDPOINT).getAddress());
        now.incrementAndGet();
        failover.connectionFailed(failover.select(ENDPOINT), new ConnectException("Connection refused"));
        assertEquals(HEALTHY, failover.select(ENDPOINT).getAddress());
        now.addAndGet(Duration.ofSeconds(30).toNanos());
        assertEquals(REFUSED, failover.select(ENDPOINT).getAddress());
    }

    /** Timeouts and application errors must retain the normal connection address and retry classification. */
    @Test
    void nonRefusalFailuresDoNotQuarantine() throws Exception {
        S3ConnectionFailover failover = policy(new AtomicLong());
        for (Throwable failure : List.of(new ConnectTimeoutException("connection timed out"),
            new ConnectException("Network is unreachable"), new IOException("Connection refused"))) {
            failover.connectionFailed(failover.select(ENDPOINT), failure);
            assertEquals(REFUSED, failover.select(ENDPOINT).getAddress());
        }
    }

    /** DNS changes discard removed failures, including late failures from older concurrent attempts. */
    @Test
    void dnsChangesDiscardOldAddressesAndTheirQuarantine() throws Exception {
        AtomicReference<InetAddress[]> answer = new AtomicReference<>(new InetAddress[] {REFUSED, HEALTHY});
        S3ConnectionFailover failover = new S3ConnectionFailover(hostname -> answer.get(), () -> 0);
        InetSocketAddress oldAttempt = failover.select(ENDPOINT);
        failover.connectionFailed(oldAttempt, new ConnectException("Connection refused"));
        answer.set(new InetAddress[] {NEW_ADDRESS});
        assertEquals(NEW_ADDRESS, failover.select(ENDPOINT).getAddress());
        failover.connectionFailed(oldAttempt, new ConnectException("Connection refused"));
        answer.set(new InetAddress[] {REFUSED, HEALTHY});
        assertEquals(REFUSED, failover.select(ENDPOINT).getAddress());
    }

    /** Refusal quarantine applies to the exact endpoint, without affecting another port or hostname. */
    @Test
    void quarantineIsScopedToHostnameAndPort() throws Exception {
        S3ConnectionFailover failover = policy(new AtomicLong());
        failover.connectionFailed(failover.select(ENDPOINT), new ConnectException("Connection refused"));
        assertEquals(REFUSED, failover.select(InetSocketAddress.createUnresolved("localhost", 9001)).getAddress());
        assertEquals(REFUSED, failover.select(InetSocketAddress.createUnresolved("other-host", 9000)).getAddress());
        assertEquals(HEALTHY, failover.select(ENDPOINT).getAddress());
    }

    private static AwsObjectStorage storage(S3ConnectionFailover failover) {
        return new AwsObjectStorage(null, "bucket") {
            @Override
            S3ConnectionFailover newConnectionFailover() {
                return failover;
            }

            @Override
            void doClose() {
                // The test owns and closes the client returned by newS3Client independently.
            }
        };
    }

    private static S3ConnectionFailover policy(AtomicLong now) {
        return new S3ConnectionFailover(hostname -> new InetAddress[] {REFUSED, HEALTHY}, now::get);
    }

    private static InetAddress address(int lastByte) {
        try {
            return InetAddress.getByAddress(new byte[] {127, 0, 0, (byte) lastByte});
        } catch (UnknownHostException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static List<Thread> nettyThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(thread -> thread.isAlive() && thread.getName().startsWith("aws-java-sdk-NettyEventLoop"))
            .collect(Collectors.toList());
    }
}
