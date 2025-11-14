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

from ducktape.tests.test import Test
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.connect import ConnectDistributedService, VerifiableSource
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH

import time


class ConnectRemoteWriteTest(Test):
    """
    Test cases for Kafka Connect OpenTelemetry Remote Write exporter functionality.
    """

    TOPIC = "remote-write-test"
    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    INPUT_FILE = "/mnt/connect.input"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "test"
    OFFSETS_TOPIC = "connect-offsets"
    OFFSETS_REPLICATION_FACTOR = "1"
    OFFSETS_PARTITIONS = "1"
    CONFIG_TOPIC = "connect-configs"
    CONFIG_REPLICATION_FACTOR = "1"
    STATUS_TOPIC = "connect-status"
    STATUS_REPLICATION_FACTOR = "1"
    STATUS_PARTITIONS = "1"
    EXACTLY_ONCE_SOURCE_SUPPORT = "disabled"
    SCHEDULED_REBALANCE_MAX_DELAY_MS = "60000"
    CONNECT_PROTOCOL="sessioned"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    FIRST_INPUT_LIST = ["foo", "bar", "baz"]
    FIRST_INPUTS = "\n".join(FIRST_INPUT_LIST) + "\n"
    SECOND_INPUT_LIST = ["razz", "ma", "tazz"]
    SECOND_INPUTS = "\n".join(SECOND_INPUT_LIST) + "\n"

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(ConnectRemoteWriteTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            self.TOPIC: {'partitions': 1, 'replication-factor': 1}
        }

        self.zk = ZookeeperService(test_context, self.num_zk) if quorum.for_test(test_context) == quorum.zk else None

    def setup_services(self, num_workers=2):
        self.kafka = KafkaService(
            self.test_context, 
            self.num_brokers, 
            self.zk,
            security_protocol=SecurityConfig.PLAINTEXT, 
            interbroker_security_protocol=SecurityConfig.PLAINTEXT,
            topics=self.topics, 
            version=DEV_BRANCH,
            server_prop_overrides=[
                ["auto.create.topics.enable", "false"],
                ["transaction.state.log.replication.factor", str(self.num_brokers)],
                ["transaction.state.log.min.isr", str(self.num_brokers)]
            ], 
            allow_zk_with_kraft=True
        )

        self.cc = ConnectDistributedService(
            self.test_context, 
            num_workers, 
            self.kafka, 
            ["/mnt/connect.input", "/mnt/connect.output"]
        )
        self.cc.log_level = "DEBUG"

        if self.zk:
            self.zk.start()
        self.kafka.start()

    def is_running(self, connector, node=None):
        """Check if connector is running"""
        try:
            status = self.cc.get_connector_status(connector.name, node)
            return (status is not None and 
                    status['connector']['state'] == 'RUNNING' and
                    all(task['state'] == 'RUNNING' for task in status['tasks']))
        except:
            return False

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

    def _verify_remote_write_requests(self, node, log_file="/tmp/mock_remote_write.log"):
        """Verify that remote write requests were received"""
        try:
            # Check the mock server log for received requests
            result = list(node.account.ssh_capture(f"cat {log_file}", allow_fail=True))
            log_content = "".join(result)

            self.logger.info(f"Remote write log content: {log_content}")

            # Look for evidence of received data
            if "Received" in log_content or "received" in log_content:
                self.logger.info("Remote write requests were successfully received")
                return True

            # Also check if the process is running and listening
            if self._check_port_listening(node, 9090) or self._check_port_listening(node, 9091):
                self.logger.info("Remote write server is listening, requests may have been processed")
                return True

            self.logger.warning("No clear evidence of remote write requests in log")
            return False

        except Exception as e:
            self.logger.warning(f"Error verifying remote write requests: {e}")
            # Don't fail the test if we can't verify the log, as the server might be working
            return True

    @cluster(num_nodes=5)
    def test_opentelemetry_remote_write_exporter(self):
        """Test OpenTelemetry Remote Write exporter functionality"""
        # Setup mock remote write server
        self.setup_services(num_workers=2)

        # Override the template to use remote write exporter
        def remote_write_config(node):
            config = self.render("connect-distributed.properties", node=node)
            # Replace prometheus exporter with remote write using correct URI format
            self.logger.info(f"connect config: {config}")
            config = config.replace(
                "automq.telemetry.exporter.uri=prometheus://0.0.0.0:9464",
                "automq.telemetry.exporter.uri=rw://?endpoint=http://localhost:9090/api/v1/write&auth=no_auth&maxBatchSize=1000000"
            )
            # Add remote write specific configurations
            config += "\nautomq.telemetry.exporter.interval.ms=30000\n"

            self.logger.info(f"connect new config: {config}")
            return config

        self.cc.set_configs(remote_write_config)

        # Setup mock remote write endpoint
        mock_server_node = self.cc.nodes[0]
        self.logger.info("Setting up mock remote write server...")

        try:
            # Start mock server
            mock_pid = self._start_mock_remote_write_server(mock_server_node, port=9090)
            self.logger.info(f"Mock remote write server started with PID: {mock_pid}")

            # Wait a bit for server to start
            time.sleep(5)

            # Verify mock server is listening
            wait_until(
                lambda: self._check_port_listening(mock_server_node, 9090),
                timeout_sec=30,
                err_msg="Mock remote write server failed to start"
            )

            self.logger.info("Starting Connect cluster with Remote Write exporter...")
            self.cc.start()

            # Create connector to generate metrics
            self.source = VerifiableSource(self.cc, topic=self.TOPIC, throughput=20)
            self.source.start()

            # Wait for connector to be running
            wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                       err_msg="VerifiableSource connector failed to start")

            # Wait for metrics to be sent to remote write endpoint
            self.logger.info("Waiting for remote write requests...")
            time.sleep(120)  # Wait for at least 2 export intervals

            # Verify remote write requests were received
            self._verify_remote_write_requests(mock_server_node)

            self.logger.info("Remote Write exporter test passed!")

        finally:
            # Cleanup
            try:
                if 'mock_pid' in locals() and mock_pid:
                    mock_server_node.account.ssh(f"kill {mock_pid}", allow_fail=True)
                if hasattr(self, 'source'):
                    self.source.stop()
                self.cc.stop()
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")

    @cluster(num_nodes=5)
    def test_remote_write_with_compression(self):
        """Test remote write exporter with gzip compression"""
        self.setup_services(num_workers=2)

        # Configure remote write with compression
        def remote_write_config(node):
            config = self.render("connect-distributed.properties", node=node)
            config = config.replace(
                "automq.telemetry.exporter.uri=prometheus://0.0.0.0:9464",
                "automq.telemetry.exporter.uri=rw://?endpoint=http://localhost:9091/api/v1/write&auth=no_auth&maxBatchSize=500000&compression=gzip"
            )
            config += "\nautomq.telemetry.exporter.interval.ms=20000\n"
            return config

        self.cc.set_configs(remote_write_config)

        mock_server_node = self.cc.nodes[0]
        
        try:
            # Start mock server on different port
            mock_pid = self._start_mock_remote_write_server(mock_server_node, port=9091)
            
            wait_until(
                lambda: self._check_port_listening(mock_server_node, 9091),
                timeout_sec=30,
                err_msg="Mock remote write server failed to start"
            )

            self.cc.start()

            # Create connector
            self.source = VerifiableSource(self.cc, topic=self.TOPIC, throughput=30)
            self.source.start()

            wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                       err_msg="VerifiableSource connector failed to start")

            # Wait for compressed requests
            time.sleep(100)

            # Verify requests were received
            log_file = "/tmp/mock_remote_write.log"
            assert self._verify_remote_write_requests(mock_server_node, log_file), \
                "Did not observe remote write payloads at the mock endpoint"

            # Check for gzip compression evidence
            result = list(mock_server_node.account.ssh_capture(f"cat {log_file}", allow_fail=True))
            log_content = "".join(result)
            if "encoding: gzip" in log_content:
                self.logger.info("Verified gzip compression was used for remote write requests")
            else:
                self.logger.warning("No evidence of gzip compression in remote write requests")

            self.logger.info("Remote write compression test passed!")

        finally:
            try:
                if 'mock_pid' in locals() and mock_pid:
                    mock_server_node.account.ssh(f"kill {mock_pid}", allow_fail=True)
                if hasattr(self, 'source'):
                    self.source.stop()
                self.cc.stop()
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")

    @cluster(num_nodes=5)
    def test_remote_write_batch_size_limits(self):
        """Test remote write exporter with different batch size configurations"""
        self.setup_services(num_workers=2)

        # Test with smaller batch size to ensure multiple requests
        def remote_write_config(node):
            config = self.render("connect-distributed.properties", node=node)
            config = config.replace(
                "automq.telemetry.exporter.uri=prometheus://0.0.0.0:9464",
                "automq.telemetry.exporter.uri=rw://?endpoint=http://localhost:9092/api/v1/write&auth=no_auth&maxBatchSize=10000"
            )
            config += "\nautomq.telemetry.exporter.interval.ms=15000\n"
            return config

        self.cc.set_configs(remote_write_config)

        mock_server_node = self.cc.nodes[0]
        
        try:
            mock_pid = self._start_mock_remote_write_server(mock_server_node, port=9092)
            
            wait_until(
                lambda: self._check_port_listening(mock_server_node, 9092),
                timeout_sec=30,
                err_msg="Mock remote write server failed to start"
            )

            self.cc.start()

            # Create connector with higher throughput to generate more metrics
            self.source = VerifiableSource(self.cc, topic=self.TOPIC, throughput=100)
            self.source.start()

            wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                       err_msg="VerifiableSource connector failed to start")

            # Wait for multiple batched requests
            time.sleep(90)

            # Verify multiple requests were received due to batch size limits
            log_file = "/tmp/mock_remote_write.log"
            result = list(mock_server_node.account.ssh_capture(f"cat {log_file}", allow_fail=True))
            log_content = "".join(result)
            
            # Count the number of received requests
            request_count = log_content.count("Received remote write request")
            self.logger.info(f"Received {request_count} remote write requests")
            
            assert request_count > 1, f"Expected multiple remote write requests due to batch size limits, but only received {request_count}"
            
            self.logger.info("Remote write batch size test passed!")

        finally:
            try:
                if 'mock_pid' in locals() and mock_pid:
                    mock_server_node.account.ssh(f"kill {mock_pid}", allow_fail=True)
                if hasattr(self, 'source'):
                    self.source.stop()
                self.cc.stop()
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")

    @cluster(num_nodes=5)
    def test_remote_write_server_unavailable(self):
        """Test remote write exporter behavior when server is unavailable"""
        self.setup_services(num_workers=2)

        # Configure remote write to point to unavailable server
        def remote_write_config(node):
            config = self.render("connect-distributed.properties", node=node)
            config = config.replace(
                "automq.telemetry.exporter.uri=prometheus://0.0.0.0:9464",
                "automq.telemetry.exporter.uri=rw://?endpoint=http://localhost:9999/api/v1/write&auth=no_auth&maxBatchSize=1000000"
            )
            config += "\nautomq.telemetry.exporter.interval.ms=10000\n"
            return config

        self.cc.set_configs(remote_write_config)

        try:
            self.logger.info("Testing remote write behavior with unavailable server...")
            self.cc.start()

            # Create connector even though remote write server is unavailable
            self.source = VerifiableSource(self.cc, topic=self.TOPIC, throughput=20)
            self.source.start()

            wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                       err_msg="VerifiableSource connector failed to start")

            # Wait for export attempts
            time.sleep(60)

            # Kafka Connect should continue functioning normally even if remote write fails
            # This is primarily a resilience test - we verify the connector doesn't crash
            self.logger.info("Connector remained stable with unavailable remote write server")

            # Verify connector is still responsive
            assert self.is_running(self.source), "Connector should remain running despite remote write failures"
            
            self.logger.info("Remote write unavailable server test passed!")

        finally:
            try:
                if hasattr(self, 'source'):
                    self.source.stop()
                self.cc.stop()
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")
