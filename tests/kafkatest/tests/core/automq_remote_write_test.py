# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService


class AutoMQRemoteWriteTest(Test):
    """End-to-end validation for AutoMQ Remote Write exporter integration."""

    TOPIC = "automq-remote-write-topic"

    def __init__(self, test_context):
        super(AutoMQRemoteWriteTest, self).__init__(test_context)
        self.num_brokers = 1
        self.zk = None
        self.kafka = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _start_kafka(self, server_overrides=None, per_node_overrides=None, extra_env=None):
        if quorum.for_test(self.test_context) == quorum.zk and self.zk is None:
            self.zk = ZookeeperService(self.test_context, 1)
            self.zk.start()

        self.kafka = KafkaService(
            self.test_context,
            self.num_brokers,
            self.zk,
            security_protocol=SecurityConfig.PLAINTEXT,
            topics={},
            server_prop_overrides=server_overrides,
            per_node_server_prop_overrides=per_node_overrides,
            extra_env=extra_env,
        )

        self.kafka.start()
        self.kafka.create_topic({
            "topic": self.TOPIC,
            "partitions": 1,
            "replication-factor": 1,
        })

    def _stop_kafka(self):
        if self.kafka is not None:
            self.kafka.stop()
            self.kafka = None
        if self.zk is not None:
            self.zk.stop()
            self.zk = None

    def _produce_messages(self, max_messages=200, throughput=1000):
        producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            kafka=self.kafka,
            topic=self.TOPIC,
            max_messages=max_messages,
            throughput=throughput,
        )
        producer.start()
        try:
            wait_until(
                lambda: producer.num_acked >= max_messages,
                timeout_sec=60,
                backoff_sec=5,
                err_msg="Producer failed to deliver expected number of messages",
            )
        finally:
            try:
                producer.stop()
            except Exception as e:
                self.logger.warn("Error stopping producer: %s", e)

    def _check_port_listening(self, node, port):
        """Check if a port is listening on the given node, with multiple fallbacks"""
        cmds = [
            f"ss -ltn | grep -E '(:|\\[::\\]):{port}\\b'",
            f"netstat -ln | grep ':{port}\\b'",
            f"lsof -iTCP:{port} -sTCP:LISTEN"
        ]
        for cmd in cmds:
            try:
                result = list(node.account.ssh_capture(cmd, allow_fail=True))
                if len(result) > 0:
                    return True
            except Exception:
                continue
        return False

    def _verify_remote_write_requests(self, node, log_file):
        """Verify that remote write requests were captured by the mock server."""
        try:
            result = list(node.account.ssh_capture(f"cat {log_file}", allow_fail=True))
            log_content = "".join(result)
            if "Received" in log_content:
                self.logger.info("Remote write server captured payloads: %s", log_content)
                return True
            self.logger.warning("No remote write payload entries detected in %s", log_file)
            return False
        except Exception as e:
            self.logger.warning("Failed to read remote write log %s: %s", log_file, e)
            return False

    def _start_mock_remote_write_server(self, node, port=9090, log_file="/tmp/mock_remote_write.log",
                                        script_file="/tmp/mock_remote_write.py"):
        """Start mock remote write HTTP server robustly"""
        # Write script file (heredoc to avoid escaping issues)
        write_cmd = f"""cat > {script_file} <<'PY'
import http.server
import socketserver
from urllib.parse import urlparse
import gzip
import sys
import time

class MockRemoteWriteHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/api/v1/write':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            encoding = self.headers.get('Content-Encoding', '')
            if encoding == 'gzip':
                try:
                    post_data = gzip.decompress(post_data)
                except Exception:
                    pass
            print(f"{{time.strftime('%Y-%m-%d-%H:%M:%S')}} - Received remote write request: {{len(post_data)}} bytes, encoding: {{encoding}}", flush=True)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            print(f"{{time.strftime('%Y-%m-%d-%H:%M:%S')}} - Received non-write request: {{self.path}}", flush=True)
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"{{time.strftime('%Y-%m-%d-%H:%M:%S')}} - HTTP: " + (format % args), flush=True)

print('Mock remote write server starting...', flush=True)
with socketserver.TCPServer(('', {port}), MockRemoteWriteHandler) as httpd:
    print('Mock remote write server listening on port {port}', flush=True)
    httpd.serve_forever()
PY"""
        node.account.ssh(write_cmd)

        # Choose python interpreter
        which_py = "PYBIN=$(command -v python3 || command -v python || echo python3)"
        # Start in background and record PID
        start_cmd = f"{which_py}; nohup $PYBIN {script_file} > {log_file} 2>&1 & echo $!"
        pid_out = list(node.account.ssh_capture(start_cmd))
        pid = pid_out[0].strip() if pid_out else None
        if not pid:
            raise RuntimeError("Failed to start mock remote write server (no PID)")

        # Wait for port to be listening
        def listening():
            if self._check_port_listening(node, port):
                return True
            # If not listening, print recent logs for troubleshooting
            try:
                tail = "".join(list(node.account.ssh_capture(f"tail -n 20 {log_file}", allow_fail=True)))
                self.logger.info(f"Mock server tail log: {tail}")
            except Exception:
                pass
            return False

        wait_until(listening, timeout_sec=30, err_msg="Mock remote write server failed to start")
        return pid

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    @cluster(num_nodes=5)
    def test_remote_write_metrics_exporter(self):
        """Verify remote write exporter integration using a mock HTTP endpoint."""
        cluster_id = f"core-remote-write-{int(time.time())}"
        remote_write_port = 19090
        log_file = f"/tmp/automq_remote_write_{int(time.time())}.log"
        script_path = f"/tmp/automq_remote_write_server_{int(time.time())}.py"

        server_overrides = [
            ["s3.telemetry.metrics.exporter.uri", f"rw://?endpoint=http://localhost:{remote_write_port}/api/v1/write&auth=no_auth&maxBatchSize=1000000"],
            ["s3.telemetry.exporter.report.interval.ms", "15000"],
            ["service.name", cluster_id],
            ["service.instance.id", "broker-remote-write"],
        ]

        remote_write_node = None
        mock_pid = None

        self._start_kafka(server_overrides=server_overrides)

        try:
            remote_write_node = self.kafka.nodes[0]
            self.logger.info("Setting up mock remote write server...")

            mock_pid = self._start_mock_remote_write_server(remote_write_node, remote_write_port, log_file, script_path)

            self.logger.info("Starting message production...")
            self._produce_messages(max_messages=400, throughput=800)

            # Allow multiple export intervals
            self.logger.info("Waiting for remote write requests...")
            time.sleep(120)

            assert self._verify_remote_write_requests(remote_write_node, log_file), \
                "Did not observe remote write payloads at the mock endpoint"
                
            self.logger.info("Remote write exporter test passed!")
        finally:
            try:
                if remote_write_node is not None and mock_pid:
                    remote_write_node.account.ssh(f"kill {mock_pid}", allow_fail=True)
                if remote_write_node is not None:
                    remote_write_node.account.ssh(f"rm -f {script_path}", allow_fail=True)
                    remote_write_node.account.ssh(f"rm -f {log_file}", allow_fail=True)
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")
            self._stop_kafka()

    @cluster(num_nodes=5)
    def test_remote_write_with_compression(self):
        """Test remote write exporter with gzip compression enabled."""
        cluster_id = f"core-remote-write-gzip-{int(time.time())}"
        remote_write_port = 19091
        log_file = f"/tmp/automq_remote_write_gzip_{int(time.time())}.log"
        script_path = f"/tmp/automq_remote_write_gzip_server_{int(time.time())}.py"

        server_overrides = [
            ["s3.telemetry.metrics.exporter.uri", f"rw://?endpoint=http://localhost:{remote_write_port}/api/v1/write&auth=no_auth&maxBatchSize=500000&compression=gzip"],
            ["s3.telemetry.exporter.report.interval.ms", "10000"],
            ["service.name", cluster_id],
            ["service.instance.id", "broker-remote-write-gzip"],
        ]

        self._start_kafka(server_overrides=server_overrides)

        try:
            remote_write_node = self.kafka.nodes[0]
            self.logger.info("Setting up mock remote write server with compression support...")

            mock_pid = self._start_mock_remote_write_server(remote_write_node, remote_write_port, log_file, script_path)

            self.logger.info("Starting message production for compression test...")
            self._produce_messages(max_messages=600, throughput=1000)

            self.logger.info("Waiting for compressed remote write requests...")
            time.sleep(90)

            # Verify requests were received
            assert self._verify_remote_write_requests(remote_write_node, log_file), \
                "Did not observe compressed remote write payloads at the mock endpoint"

            # Check that gzip encoding was used
            result = list(remote_write_node.account.ssh_capture(f"cat {log_file}", allow_fail=True))
            log_content = "".join(result)
            if "encoding: gzip" in log_content:
                self.logger.info("Verified gzip compression was used for remote write requests")
            else:
                self.logger.warning("No evidence of gzip compression in remote write requests")

            self.logger.info("Remote write compression test passed!")
        finally:
            try:
                if 'remote_write_node' in locals() and 'mock_pid' in locals() and mock_pid:
                    remote_write_node.account.ssh(f"kill {mock_pid}", allow_fail=True)
                if 'remote_write_node' in locals():
                    remote_write_node.account.ssh(f"rm -f {script_path}", allow_fail=True)
                    remote_write_node.account.ssh(f"rm -f {log_file}", allow_fail=True)
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")
            self._stop_kafka()

    @cluster(num_nodes=5)
    def test_remote_write_batch_size_limits(self):
        """Test remote write exporter with different batch size configurations."""
        cluster_id = f"core-remote-write-batch-{int(time.time())}"
        remote_write_port = 19092
        log_file = f"/tmp/automq_remote_write_batch_{int(time.time())}.log"
        script_path = f"/tmp/automq_remote_write_batch_server_{int(time.time())}.py"

        # Test with smaller batch size to ensure multiple requests
        server_overrides = [
            ["s3.telemetry.metrics.exporter.uri", f"rw://?endpoint=http://localhost:{remote_write_port}/api/v1/write&auth=no_auth&maxBatchSize=10000"],
            ["s3.telemetry.exporter.report.interval.ms", "5000"],
            ["service.name", cluster_id],
            ["service.instance.id", "broker-remote-write-batch"],
        ]

        self._start_kafka(server_overrides=server_overrides)

        try:
            remote_write_node = self.kafka.nodes[0]
            self.logger.info("Setting up mock remote write server for batch size testing...")

            mock_pid = self._start_mock_remote_write_server(remote_write_node, remote_write_port, log_file, script_path)

            self.logger.info("Starting high-volume message production...")
            # Produce more messages to trigger multiple batches
            self._produce_messages(max_messages=1000, throughput=2000)

            self.logger.info("Waiting for multiple batched remote write requests...")
            time.sleep(60)

            # Verify multiple requests were received due to batch size limits
            result = list(remote_write_node.account.ssh_capture(f"cat {log_file}", allow_fail=True))
            log_content = "".join(result)
            
            # Count the number of received requests
            request_count = log_content.count("Received remote write request")
            self.logger.info(f"Received {request_count} remote write requests")
            
            assert request_count > 1, f"Expected multiple remote write requests due to batch size limits, but only received {request_count}"
            
            self.logger.info("Remote write batch size test passed!")
        finally:
            try:
                if 'remote_write_node' in locals() and 'mock_pid' in locals() and mock_pid:
                    remote_write_node.account.ssh(f"kill {mock_pid}", allow_fail=True)
                if 'remote_write_node' in locals():
                    remote_write_node.account.ssh(f"rm -f {script_path}", allow_fail=True)
                    remote_write_node.account.ssh(f"rm -f {log_file}", allow_fail=True)
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")
            self._stop_kafka()

    @cluster(num_nodes=5)
    def test_remote_write_server_unavailable(self):
        """Test remote write exporter behavior when server is unavailable."""
        cluster_id = f"core-remote-write-unavail-{int(time.time())}"
        # Use a port that we won't start a server on
        remote_write_port = 19093

        server_overrides = [
            ["s3.telemetry.metrics.exporter.uri", f"rw://?endpoint=http://localhost:{remote_write_port}/api/v1/write&auth=no_auth&maxBatchSize=1000000"],
            ["s3.telemetry.exporter.report.interval.ms", "10000"],
            ["service.name", cluster_id],
            ["service.instance.id", "broker-remote-write-unavail"],
        ]

        self._start_kafka(server_overrides=server_overrides)

        try:
            self.logger.info("Testing remote write behavior with unavailable server...")

            # Produce messages even though remote write server is unavailable
            self._produce_messages(max_messages=200, throughput=500)

            # Wait for export attempts
            time.sleep(30)

            # Kafka should continue functioning normally even if remote write fails
            # This is primarily a resilience test - we verify the broker doesn't crash
            self.logger.info("Broker remained stable with unavailable remote write server")

            # Verify broker is still responsive
            final_messages = 100
            self._produce_messages(max_messages=final_messages, throughput=200)
            
            self.logger.info("Remote write unavailable server test passed!")
        finally:
            self._stop_kafka()
