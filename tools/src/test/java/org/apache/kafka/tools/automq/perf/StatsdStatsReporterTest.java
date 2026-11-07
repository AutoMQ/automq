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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class StatsdStatsReporterTest {

    /** Given an interval with produced and consumed traffic, the receiver gets the exact rates and bounded tags. */
    @Test
    void reportsStatsCollectorRatesToLocalReceiver() throws Exception {
        try (DatagramSocket receiver = new DatagramSocket(0, InetAddress.getByName("127.0.0.1"));
             DatagramSocket sender = new DatagramSocket();
             StatsdStatsReporter reporter = new StatsdStatsReporter(sender,
                 new InetSocketAddress("127.0.0.1", receiver.getLocalPort()),
                 "test-cluster", "us-west-2a")) {
            receiver.setSoTimeout(2000);
            StatsCollector.Result result = collectOneInterval(reporter);

            String payload = receivePayload(receiver) + receivePayload(receiver);
            Map<String, Double> metrics = Arrays.stream(payload.strip().split("\\n"))
                .collect(Collectors.toMap(line -> line.substring(0, line.indexOf(':')),
                    line -> Double.parseDouble(line.substring(line.indexOf(':') + 1, line.indexOf('|')))));

            assertEquals(11, metrics.size());
            assertEquals(result.produceThroughputBps.get(0), metrics.get("kafka.automq_perf.produced_bytes_per_second"));
            assertEquals(result.consumeThroughputBps.get(0), metrics.get("kafka.automq_perf.consumed_bytes_per_second"));
            assertEquals(result.produceRate.get(0), metrics.get("kafka.automq_perf.producer_messages_per_second"));
            assertEquals(result.consumeRate.get(0), metrics.get("kafka.automq_perf.consumer_messages_per_second"));
            assertEquals(result.errorRate.get(0), metrics.get("kafka.automq_perf.producer_errors_per_second"));
            assertEquals(result.endToEndLatencyMeanMicros.get(0) / 1000,
                metrics.get("kafka.automq_perf.end_to_end_latency_avg_ms"));
            assertEquals(result.endToEndLatencyMinMicros.get(0) / 1000,
                metrics.get("kafka.automq_perf.end_to_end_latency_min_ms"));
            assertEquals(result.endToEndLatency50thMicros.get(0) / 1000,
                metrics.get("kafka.automq_perf.end_to_end_latency_p50_ms"));
            assertEquals(result.endToEndLatency99thMicros.get(0) / 1000,
                metrics.get("kafka.automq_perf.end_to_end_latency_p99_ms"));
            assertEquals(result.endToEndLatency999thMicros.get(0) / 1000,
                metrics.get("kafka.automq_perf.end_to_end_latency_p999_ms"));
            assertEquals(result.endToEndLatencyMaxMicros.get(0) / 1000,
                metrics.get("kafka.automq_perf.end_to_end_latency_max_ms"));
            for (String line : payload.strip().split("\\n")) {
                assertTrue(line.endsWith("|g|#kafka_cluster:test-cluster,source_availability_zone:us-west-2a"));
                assertFalse(line.contains("run_id"));
                assertFalse(line.contains("partition"));
            }
            // The existing result remains a plain data object for PerfCommand's final JSON writer.
            String json = new ObjectMapper().writeValueAsString(result);
            assertTrue(json.contains("\"produceThroughputBps\""));
            assertTrue(json.contains("\"consumeThroughputBps\""));
            assertFalse(json.contains("StatsdStatsReporter"));
        }
    }

    /** Given an emitter failure, interval collection and its final JSON still complete. */
    @Test
    void emitterFailureDoesNotInterruptStatsCollection() throws Exception {
        try (DatagramSocket sender = new DatagramSocket() {
            @Override
            public void send(DatagramPacket packet) throws IOException {
                throw new SocketException("StatsD unavailable");
            }
        };
             StatsdStatsReporter reporter = new StatsdStatsReporter(sender,
                 new InetSocketAddress("127.0.0.1", 8200), "test-cluster", null)) {
            StatsCollector.Result result = collectOneInterval(reporter);
            assertEquals(1, result.produceRate.size());
            assertEquals(1, result.consumeRate.size());
            assertEquals(1, result.endToEndLatency99thMicros.size());
            assertTrue(new ObjectMapper().writeValueAsString(result).contains("\"produceRate\""));
        }
    }

    /** Given no consumed records, the receiver gets rates without a misleading zero latency. */
    @Test
    void skipsEndToEndLatencyWhenNoRecordsWereConsumed() throws Exception {
        try (DatagramSocket receiver = new DatagramSocket(0, InetAddress.getByName("127.0.0.1"));
             DatagramSocket sender = new DatagramSocket();
             StatsdStatsReporter reporter = new StatsdStatsReporter(sender,
                 new InetSocketAddress("127.0.0.1", receiver.getLocalPort()),
                 "test-cluster", null)) {
            receiver.setSoTimeout(100);
            StatsCollector.printAndCollectStats(new Stats(), (start, now) -> true,
                20_000_000, new PerfConfig(new String[0]), reporter);
            assertFalse(receivePayload(receiver).contains("end_to_end_latency"));
            byte[] data = new byte[2048];
            DatagramPacket packet = new DatagramPacket(data, data.length);
            assertThrows(SocketTimeoutException.class, () -> receiver.receive(packet));
        }
    }

    private static String receivePayload(DatagramSocket receiver) throws IOException {
        byte[] data = new byte[2048];
        DatagramPacket packet = new DatagramPacket(data, data.length);
        receiver.receive(packet);
        return new String(packet.getData(), 0, packet.getLength(), StandardCharsets.US_ASCII);
    }

    private static StatsCollector.Result collectOneInterval(StatsdStatsReporter reporter) {
        Stats stats = new Stats();
        long sendTimeNanos = StatsCollector.currentNanos() - 1_000_000;
        stats.messageSent(4096, sendTimeNanos);
        stats.messageReceived(2, 2048, sendTimeNanos);
        stats.messageReceived(1, 1024, sendTimeNanos - 1_000_000);
        stats.messageFailed();
        return StatsCollector.printAndCollectStats(stats, (start, now) -> true,
            20_000_000, new PerfConfig(new String[0]), reporter);
    }
}
