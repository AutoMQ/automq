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

package org.apache.kafka.tools.automq.perf;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

/**
 * Sends best-effort interval gauges to a host-local StatsD listener. Reporting is
 * disabled unless the caller supplies {@code AUTOMQ_PERF_STATSD_PORT}; failures
 * never change the outcome of a performance test.
 */
final class StatsdStatsReporter implements AutoCloseable {
    private static final String PORT_ENV = "AUTOMQ_PERF_STATSD_PORT";
    private static final String CLUSTER_ENV = "AUTOMQ_PERF_STATSD_CLUSTER";
    private static final String AZ_ENV = "AUTOMQ_PERF_STATSD_AZ";

    private final DatagramSocket socket;
    private final SocketAddress address;
    private final String tags;

    private StatsdStatsReporter() {
        socket = null;
        address = null;
        tags = null;
    }

    StatsdStatsReporter(DatagramSocket socket, SocketAddress address, String cluster, String az) {
        this.socket = socket;
        this.address = address;
        StringJoiner tagValues = new StringJoiner(",");
        if (validTagValue(cluster)) {
            tagValues.add("kafka_cluster:" + cluster);
        }
        if (validTagValue(az)) {
            tagValues.add("source_availability_zone:" + az);
        }
        this.tags = tagValues.length() == 0 ? "" : "|#" + tagValues;
    }

    static StatsdStatsReporter fromEnvironment() {
        String portValue = System.getenv(PORT_ENV);
        if (portValue == null) {
            return new StatsdStatsReporter();
        }
        int port;
        try {
            port = Integer.parseInt(portValue);
        } catch (NumberFormatException ignored) {
            return new StatsdStatsReporter();
        }
        if (port < 1 || port > 65535) {
            return new StatsdStatsReporter();
        }
        try {
            return new StatsdStatsReporter(new DatagramSocket(),
                new InetSocketAddress("127.0.0.1", port),
                System.getenv(CLUSTER_ENV), System.getenv(AZ_ENV));
        } catch (SocketException | SecurityException ignored) {
            return new StatsdStatsReporter();
        }
    }

    private static boolean validTagValue(String value) {
        return value != null && value.length() <= 64 && value.matches("[A-Za-z0-9][A-Za-z0-9-]*");
    }

    void emit(double producedBytesPerSecond, double consumedBytesPerSecond,
        double producerMessagesPerSecond, double consumerMessagesPerSecond, double producerErrorsPerSecond) {
        if (socket == null) {
            return;
        }
        String payload = metric("produced_bytes_per_second", producedBytesPerSecond)
            + metric("consumed_bytes_per_second", consumedBytesPerSecond)
            + metric("producer_messages_per_second", producerMessagesPerSecond)
            + metric("consumer_messages_per_second", consumerMessagesPerSecond)
            + metric("producer_errors_per_second", producerErrorsPerSecond);
        send(payload);
    }

    void emitEndToEndLatency(double averageMs, double minimumMs, double p50Ms,
        double p99Ms, double p999Ms, double maximumMs) {
        if (socket == null) {
            return;
        }
        String payload = metric("end_to_end_latency_avg_ms", averageMs)
            + metric("end_to_end_latency_min_ms", minimumMs)
            + metric("end_to_end_latency_p50_ms", p50Ms)
            + metric("end_to_end_latency_p99_ms", p99Ms)
            + metric("end_to_end_latency_p999_ms", p999Ms)
            + metric("end_to_end_latency_max_ms", maximumMs);
        send(payload);
    }

    private void send(String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.US_ASCII);
        try {
            socket.send(new DatagramPacket(bytes, bytes.length, address));
        } catch (IOException | RuntimeException ignored) {
            // A missing or unhealthy local listener cannot affect load generation.
        }
    }

    private String metric(String name, double value) {
        return "kafka.automq_perf." + name + ':' + Double.toString(value) + "|g" + tags + '\n';
    }

    @Override
    public void close() {
        if (socket != null) {
            socket.close();
        }
    }
}
