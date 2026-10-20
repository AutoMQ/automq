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

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Avoids recently refused addresses when the SDK retries an S3 connection. Each HTTP client owns one
 * instance; all of its channels share bounded, synchronized failure state. Only the TCP destination
 * changes, so the SDK retains the original hostname for TLS, signing and HTTP. No request is replayed here.
 */
final class S3ConnectionFailover implements ChannelFactory<SocketChannel> {
    private static final int MAX_ENDPOINTS = 128;
    private static final long COOLDOWN_NANOS = Duration.ofSeconds(30).toNanos();

    private final AddressResolver resolver;
    private final LongSupplier nanoTime;
    private final Map<InetSocketAddress, EndpointState> endpoints = new LinkedHashMap<>(16, 0.75f, true);

    S3ConnectionFailover() {
        this(InetAddress::getAllByName, System::nanoTime);
    }

    S3ConnectionFailover(AddressResolver resolver, LongSupplier nanoTime) {
        this.resolver = resolver;
        this.nanoTime = nanoTime;
    }

    @Override
    public SocketChannel newChannel() {
        SocketChannel channel = new NioSocketChannel();
        channel.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
            @Override
            public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                ChannelPromise promise) throws Exception {
                if (!(remoteAddress instanceof InetSocketAddress)) {
                    ctx.connect(remoteAddress, localAddress, promise);
                    return;
                }
                InetSocketAddress destination = select((InetSocketAddress) remoteAddress);
                // SDK listeners may already be attached to the original promise. Complete a separate
                // transport promise first so even an immediate retry observes the recorded refusal.
                ChannelPromise transportPromise = ctx.newPromise();
                promise.addListener(future -> {
                    if (future.isCancelled()) {
                        transportPromise.cancel(false);
                        ctx.close();
                    }
                });
                transportPromise.addListener(future -> {
                    if (future.isSuccess()) {
                        if (!promise.trySuccess()) {
                            ctx.close();
                        }
                    } else if (future.isCancelled()) {
                        promise.cancel(false);
                        ctx.close();
                    } else {
                        connectionFailed(destination, future.cause());
                        promise.tryFailure(future.cause());
                    }
                });
                ctx.connect(destination, localAddress, transportPromise);
            }
        });
        return channel;
    }

    InetSocketAddress select(InetSocketAddress original) throws UnknownHostException {
        String hostname = original.getHostString();
        InetAddress[] addresses;
        try {
            // The SDK has just used the same JVM resolver, whose cached answer contains all A/AAAA records.
            addresses = resolver.resolve(hostname);
        } catch (UnknownHostException e) {
            if (!original.isUnresolved()) {
                return original;
            }
            throw e;
        }
        if (addresses.length == 0) {
            return original;
        }
        InetSocketAddress endpoint = InetSocketAddress.createUnresolved(hostname, original.getPort());
        InetAddress selected = addresses[0];
        long now = nanoTime.getAsLong();
        synchronized (endpoints) {
            EndpointState state = endpoints.computeIfAbsent(endpoint, ignored -> new EndpointState());
            state.addresses = Arrays.asList(addresses);
            state.failures.keySet().retainAll(state.addresses);
            state.failures.values().removeIf(failedAt -> now - failedAt >= COOLDOWN_NANOS);
            long oldestFailureAge = Long.MIN_VALUE;
            for (InetAddress address : addresses) {
                Long failedAt = state.failures.get(address);
                if (failedAt == null) {
                    selected = address;
                    break;
                }
                // If every address has failed, keep probing rather than making the quarantine an outage.
                long age = now - failedAt;
                if (age > oldestFailureAge) {
                    oldestFailureAge = age;
                    selected = address;
                }
            }
            if (endpoints.size() > MAX_ENDPOINTS) {
                endpoints.remove(endpoints.keySet().iterator().next());
            }
        }
        // Preserve the hostname without triggering a reverse DNS lookup.
        return new InetSocketAddress(InetAddress.getByAddress(hostname, selected.getAddress()), original.getPort());
    }

    void connectionFailed(InetSocketAddress destination, Throwable failure) {
        if (!isConnectionRefused(failure)) {
            return;
        }
        InetSocketAddress endpoint = InetSocketAddress.createUnresolved(destination.getHostString(), destination.getPort());
        synchronized (endpoints) {
            EndpointState state = endpoints.get(endpoint);
            // An older in-flight attempt must not reintroduce an address removed by a newer DNS answer.
            if (state != null && state.addresses.contains(destination.getAddress())) {
                state.failures.put(destination.getAddress(), nanoTime.getAsLong());
            }
        }
    }

    private static boolean isConnectionRefused(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ConnectException && cause.getMessage() != null
                && cause.getMessage().toLowerCase(Locale.ROOT).contains("connection refused")) {
                return true;
            }
        }
        return false;
    }

    @FunctionalInterface
    interface AddressResolver {
        InetAddress[] resolve(String hostname) throws UnknownHostException;
    }

    private static class EndpointState {
        private List<InetAddress> addresses = List.of();
        private final Map<InetAddress, Long> failures = new HashMap<>();
    }
}
