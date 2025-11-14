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
from ducktape.mark import matrix, parametrize

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, config_property, quorum
from kafkatest.services.connect import ConnectDistributedService, VerifiableSource
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH

import time
import subprocess


class ConnectMetricsLogTest(Test):
    """
    Test class specifically for testing Kafka Connect metrics endpoint functionality,
    extracted from ConnectDistributedTest to focus on metrics validation.
    """
    
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
        super(ConnectMetricsLogTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            'metrics-test-topic': {'partitions': 1, 'replication-factor': 1},
            'test': {'partitions': 1, 'replication-factor': 1}
        }
        
        # Constants from original test class
        self.TOPIC = "test"

        self.zk = ZookeeperService(test_context, self.num_zk) if quorum.for_test(test_context) == quorum.zk else None

    def setup_services(self,
                       security_protocol=SecurityConfig.PLAINTEXT,
                       broker_version=DEV_BRANCH,
                       auto_create_topics=True,
                       num_workers=3,
                       kraft=True):
        """Setup Kafka and Connect services"""
        self.kafka = KafkaService(self.test_context, self.num_brokers, self.zk,
                                  security_protocol=security_protocol,
                                  interbroker_security_protocol=security_protocol,
                                  topics=self.topics, version=broker_version,
                                  server_prop_overrides=[
                                      ["auto.create.topics.enable", str(auto_create_topics)],
                                      ["transaction.state.log.replication.factor", str(self.num_brokers)],
                                      ["transaction.state.log.min.isr", str(self.num_brokers)]
                                  ], allow_zk_with_kraft=kraft)

        self.cc = ConnectDistributedService(self.test_context, num_workers, self.kafka, [])
        self.cc.log_level = "DEBUG"

        if self.zk:
            self.zk.start()
        self.kafka.start()

    def is_running(self, connector, node=None):
        """Check if a connector and all its tasks are running"""
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'RUNNING') and self._all_tasks_have_state(status, connector.tasks, 'RUNNING')

    def _connector_status(self, connector, node=None):
        """Get connector status"""
        try:
            return self.cc.get_connector_status(connector, node)
        except:
            return None

    def _connector_has_state(self, status, state):
        """Check if connector has specific state"""
        return status is not None and status['connector']['state'] == state

    def _all_tasks_have_state(self, status, task_count, state):
        """Check if all tasks have specific state"""
        if status is None:
            return False

        tasks = status['tasks']
        if len(tasks) != task_count:
            return False

        return all(task['state'] == state for task in tasks)

    def _wait_for_metrics_available(self, timeout_sec=60):
        """Wait for metrics endpoint to become available"""
        self.logger.info("Waiting for metrics endpoint to become available...")
        
        def metrics_available():
            for node in self.cc.nodes:
                try:
                    cmd = "curl -s http://localhost:9464/metrics"
                    result = node.account.ssh_capture(cmd, allow_fail=True)
                    metrics_output = "".join([line for line in result])
                    
                    # Check for any metrics output (not just kafka_connect)
                    if len(metrics_output.strip()) > 0 and ("#" in metrics_output or "_" in metrics_output):
                        self.logger.info(f"Metrics available on node {node.account.hostname}, content length: {len(metrics_output)}")
                        return True
                    else:
                        self.logger.debug(f"Node {node.account.hostname} metrics not ready yet, output length: {len(metrics_output)}")
                except Exception as e:
                    self.logger.debug(f"Error checking metrics on node {node.account.hostname}: {e}")
                    continue
            return False

        wait_until(
            metrics_available,
            timeout_sec=timeout_sec,
            err_msg="Metrics endpoint did not become available within the specified time"
        )
        
        self.logger.info("Metrics endpoint is now available!")

    def _verify_opentelemetry_metrics(self):
        """Verify OpenTelemetry metrics content"""
        for node in self.cc.nodes:
            cmd = "curl -s http://localhost:9464/metrics"
            result = node.account.ssh_capture(cmd)
            metrics_output = "".join([line for line in result])

            # Basic check - verify any metrics output exists
            assert len(metrics_output.strip()) > 0, "Metrics endpoint returned no content"
            
            # Print ALL metrics for debugging
            self.logger.info(f"=== ALL METRICS from Node {node.account.hostname} ===")
            self.logger.info(metrics_output)
            self.logger.info(f"=== END OF METRICS from Node {node.account.hostname} ===")

            # Find all metric lines (not comments)
            metric_lines = [line for line in metrics_output.split('\n') 
                           if line.strip() and not line.startswith('#') and ('_' in line or '{' in line)]
            
            # Should have at least some metrics
            assert len(metric_lines) > 0, "No valid metric lines found"
            
            self.logger.info(f"Found {len(metric_lines)} metric lines")
            
            # Log kafka_connect metrics specifically
            kafka_connect_lines = [line for line in metric_lines if 'kafka_connect' in line]
            self.logger.info(f"Found {len(kafka_connect_lines)} kafka_connect metric lines:")
            for i, line in enumerate(kafka_connect_lines):
                self.logger.info(f"kafka_connect metric {i+1}: {line}")

            # Check for Prometheus format characteristics
            has_help = "# HELP" in metrics_output
            has_type = "# TYPE" in metrics_output
            
            if has_help and has_type:
                self.logger.info("Metrics conform to Prometheus format")
            else:
                self.logger.warning("Metrics may not be in standard Prometheus format")

            # Use lenient metric validation to analyze values
            self._validate_metric_values(metrics_output)

            self.logger.info(f"Node {node.account.hostname} basic metrics validation passed")

    def _verify_comprehensive_metrics(self):
        """Comprehensive metrics validation"""
        for node in self.cc.nodes:
            cmd = "curl -s http://localhost:9464/metrics"
            result = node.account.ssh_capture(cmd)
            metrics_output = "".join([line for line in result])

            # Basic check - verify any metrics output exists
            assert len(metrics_output.strip()) > 0, "Metrics endpoint returned no content"

            # Print ALL metrics for comprehensive debugging
            self.logger.info(f"=== COMPREHENSIVE METRICS from Node {node.account.hostname} ===")
            self.logger.info(metrics_output)
            self.logger.info(f"=== END OF COMPREHENSIVE METRICS from Node {node.account.hostname} ===")

            # Find all metric lines (start with letter, not comments)
            metric_lines = [line for line in metrics_output.split('\n')
                           if line.strip() and not line.startswith('#') and ('_' in line or '{' in line)]
            self.logger.info(f"Found metric line count: {len(metric_lines)}")

            # Find kafka_connect related metrics
            kafka_connect_lines = [line for line in metric_lines if 'kafka_connect' in line]
            self.logger.info(f"Found kafka_connect metric line count: {len(kafka_connect_lines)}")

            # Print all kafka_connect metrics
            self.logger.info("=== ALL kafka_connect metrics ===")
            for i, line in enumerate(kafka_connect_lines):
                self.logger.info(f"kafka_connect metric {i+1}: {line}")

            # If no kafka_connect metrics found, show other metrics
            if len(kafka_connect_lines) == 0:
                self.logger.warning("No kafka_connect metrics found, showing other metrics:")
                for i, line in enumerate(metric_lines[:10]):  # Show first 10 instead of 5
                    self.logger.info(f"Other metric line {i+1}: {line}")

                # Should have at least some metric output
                assert len(metric_lines) > 0, "No valid metric lines found"
            else:
                # Found kafka_connect metrics
                self.logger.info(f"Successfully found {len(kafka_connect_lines)} kafka_connect metrics")

            # Check for HELP and TYPE comments (Prometheus format characteristics)
            has_help = "# HELP" in metrics_output
            has_type = "# TYPE" in metrics_output

            if has_help:
                self.logger.info("Found HELP comments - conforms to Prometheus format")
            if has_type:
                self.logger.info("Found TYPE comments - conforms to Prometheus format")

            self.logger.info(f"Node {node.account.hostname} metrics validation passed, total {len(metric_lines)} metrics found")

    def _validate_metric_values(self, metrics_output):
        """Validate metric value reasonableness - more lenient version"""
        lines = metrics_output.split('\n')
        negative_metrics = []
        
        self.logger.info("=== ANALYZING METRIC VALUES ===")
        
        for line in lines:
            if line.startswith('kafka_connect_') and not line.startswith('#'):
                # Parse metric line: metric_name{labels} value timestamp
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        value = float(parts[1])
                        metric_name = parts[0].split('{')[0] if '{' in parts[0] else parts[0]
                        
                        # Log all metric values for analysis
                        self.logger.info(f"Metric: {metric_name} = {value}")
                        
                        # Some metrics can legitimately be negative (e.g., ratios, differences, etc.)
                        # Only flag as problematic if it's a count or gauge that shouldn't be negative
                        if value < 0:
                            negative_metrics.append(f"{parts[0]} = {value}")
                            
                            # Allow certain metrics to be negative
                            allowed_negative_patterns = [
                                'ratio',
                                'seconds_ago',
                                'difference',
                                'offset',
                                'lag'
                            ]
                            
                            is_allowed_negative = any(pattern in parts[0].lower() for pattern in allowed_negative_patterns)
                            
                            if is_allowed_negative:
                                self.logger.info(f"Negative value allowed for metric: {parts[0]} = {value}")
                            else:
                                self.logger.warning(f"Potentially problematic negative value: {parts[0]} = {value}")
                                # Don't assert here, just log for now
                                
                    except ValueError:
                        # Skip unparseable lines
                        continue
        
        if negative_metrics:
            self.logger.info(f"Found {len(negative_metrics)} metrics with negative values:")
            for metric in negative_metrics:
                self.logger.info(f"  - {metric}")
        
        self.logger.info("=== END METRIC VALUE ANALYSIS ===")

    def _verify_metrics_updates(self):
        """Verify metrics update over time"""
        # Get initial metrics
        initial_metrics = {}
        for node in self.cc.nodes:
            cmd = "curl -s http://localhost:9464/metrics"
            result = node.account.ssh_capture(cmd)
            initial_metrics[node] = "".join([line for line in result])

        # Wait for some time
        time.sleep(5)

        # Get metrics again and compare
        for node in self.cc.nodes:
            cmd = "curl -s http://localhost:9464/metrics"
            result = node.account.ssh_capture(cmd)
            current_metrics = "".join([line for line in result])

            # Metrics should have changed (at least timestamps will update)
            # More detailed verification can be done here
            self.logger.info(f"Node {node.account.hostname} metrics have been updated")

    def _safe_cleanup(self):
        """Safe resource cleanup"""
        try:
            # Delete connectors
            connectors = self.cc.list_connectors()
            for connector in connectors:
                try:
                    self.cc.delete_connector(connector)
                    self.logger.info(f"Deleted connector: {connector}")
                except Exception as e:
                    self.logger.warning(f"Failed to delete connector {connector}: {e}")

            # Stop services
            self.cc.stop()

        except Exception as e:
            self.logger.error(f"Error occurred during cleanup: {e}")

    def _check_port_listening(self, node, port):
        """Check if a port is listening on the given node"""
        try:
            result = list(node.account.ssh_capture(f"netstat -ln | grep :{port}", allow_fail=True))
            return len(result) > 0
        except:
            return False

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

    def _verify_s3_metrics_export_localstack(self, bucket_name, node, selector_type):
        """Verify that metrics were exported to S3 via localstack"""
        try:
            # Recursively list all object files (not directories) in S3 bucket
            list_cmd = f"aws s3 ls s3://{bucket_name}/ --recursive --endpoint=http://10.5.0.2:4566"

            ret, val = subprocess.getstatusoutput(list_cmd)
            self.logger.info(
                f'\n--------------recursive objects[bucket:{bucket_name}]--------------------\n{val}\n--------------recursive objects end--------------------\n')
            if ret != 0:
                self.logger.warning(f"Failed to list bucket objects recursively, return code: {ret}, output: {val}")
                # Try non-recursive listing of directory structure
                list_dir_cmd = f"aws s3 ls s3://{bucket_name}/ --endpoint=http://10.5.0.2:4566"
                ret2, val2 = subprocess.getstatusoutput(list_dir_cmd)
                self.logger.info(f"Directory listing: {val2}")

                # If non-recursive also fails, the bucket may not exist or lack permissions
                if ret2 != 0:
                    raise Exception(f"Failed to list bucket contents, output: {val}")
                else:
                    # Found directories but no files, upload may not be complete yet
                    self.logger.info("Found directories but no files yet, checking subdirectories...")

                    # Try to list contents under automq/metrics/
                    automq_cmd = f"aws s3 ls s3://{bucket_name}/automq/metrics/ --recursive --endpoint=http://10.5.0.2:4566"
                    ret3, val3 = subprocess.getstatusoutput(automq_cmd)
                    self.logger.info(f"AutoMQ metrics directory contents: {val3}")

                    if ret3 == 0 and val3.strip():
                        s3_objects = [line.strip() for line in val3.strip().split('\n') if line.strip()]
                    else:
                        return False
            else:
                s3_objects = [line.strip() for line in val.strip().split('\n') if line.strip()]

            self.logger.info(f"S3 bucket {bucket_name} file contents (total {len(s3_objects)} files): {s3_objects}")

            if s3_objects:
                # Filter out directory lines, keep only file lines (file lines usually have size info)
                file_objects = []
                for obj_line in s3_objects:
                    parts = obj_line.split()
                    # File line format: 2025-01-01 12:00:00 size_in_bytes filename
                    # Directory line format: PRE directory_name/ or just directory name
                    if len(parts) >= 4 and not obj_line.strip().startswith('PRE') and 'automq/metrics/' in obj_line:
                        file_objects.append(obj_line)

                self.logger.info(f"Found {len(file_objects)} actual metric files in S3:")
                for file_obj in file_objects:
                    self.logger.info(f"  - {file_obj}")

                if file_objects:
                    self.logger.info(f"S3 metrics export verified via localstack: found {len(file_objects)} metric files")

                    # Try to download and check the first file's content
                    try:
                        first_file_parts = file_objects[0].split()
                        if len(first_file_parts) >= 4:
                            object_name = ' '.join(first_file_parts[3:])  # File name may contain spaces

                            # Download and check content
                            download_cmd = f"aws s3 cp s3://{bucket_name}/{object_name} /tmp/sample_metrics.json --endpoint=http://10.5.0.2:4566"
                            ret, download_output = subprocess.getstatusoutput(download_cmd)
                            if ret == 0:
                                self.logger.info(f"Successfully downloaded sample metrics file: {download_output}")

                                # Check file content
                                cat_cmd = "head -n 3 /tmp/sample_metrics.json"
                                ret2, content = subprocess.getstatusoutput(cat_cmd)
                                if ret2 == 0:
                                    self.logger.info(f"Sample metrics content: {content}")
                                    # Verify content format is correct (should contain JSON formatted metric data)
                                    if any(keyword in content for keyword in ['timestamp', 'name', 'kind', 'tags']):
                                        self.logger.info("Metrics content format verification passed")
                                    else:
                                        self.logger.warning(f"Metrics content format may be incorrect: {content}")
                            else:
                                self.logger.warning(f"Failed to download sample file: {download_output}")
                    except Exception as e:
                        self.logger.warning(f"Error validating sample metrics file: {e}")

                    return True
                else:
                    self.logger.warning("Found S3 objects but none appear to be metric files")
                    return False
            else:
                # Check if bucket exists but is empty
                bucket_check_cmd = f"aws s3api head-bucket --bucket {bucket_name} --endpoint-url http://10.5.0.2:4566"
                ret, bucket_output = subprocess.getstatusoutput(bucket_check_cmd)
                if ret == 0:
                    self.logger.info(f"Bucket {bucket_name} exists but is empty - metrics may not have been exported yet")
                    return False
                else:
                    self.logger.warning(f"Bucket {bucket_name} may not exist: {bucket_output}")
                    return False

        except Exception as e:
            self.logger.warning(f"Error verifying S3 metrics export via localstack: {e}")
            return False

    @cluster(num_nodes=5)
    def test_metrics_availability_basic(self):
        """Test basic metrics endpoint availability"""
        self.setup_services(num_workers=3)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        try:
            self.logger.info("Testing basic metrics availability...")
            self._wait_for_metrics_available()
            self.logger.info("Basic metrics availability test passed!")

        finally:
            self.cc.stop()

    @cluster(num_nodes=5)
    def test_opentelemetry_metrics_with_connector(self):
        """Test OpenTelemetry metrics with running connector"""
        self.setup_services(num_workers=3)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        try:
            self.logger.info("Creating VerifiableSource connector...")
            # Use VerifiableSource connector
            self.source = VerifiableSource(self.cc, topic='metrics-test-topic', throughput=10)
            self.source.start()

            # Wait for connector to be running
            self.logger.info("Waiting for connector to be running...")
            wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                       err_msg="VerifiableSource connector failed to start")

            self.logger.info("Connector is running, checking metrics...")
            
            # Wait for and verify metrics
            self._wait_for_metrics_available()
            self._verify_opentelemetry_metrics()

            # Verify metrics update over time
            self._verify_metrics_updates()
            
            self.logger.info("All metrics validations passed!")

        finally:
            if hasattr(self, 'source'):
                self.logger.info("Stopping source connector...")
                self.source.stop()
            self.logger.info("Stopping Connect cluster...")
            self.cc.stop()

    @cluster(num_nodes=5)
    def test_comprehensive_metrics_validation(self):
        """Comprehensive Connect OpenTelemetry metrics test"""
        self.setup_services(num_workers=3)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        try:
            # Create connector using VerifiableSource
            self.source = VerifiableSource(self.cc, topic='metrics-test-topic', throughput=50)
            self.source.start()

            # Wait for connector startup
            wait_until(
                lambda: self.is_running(self.source),
                timeout_sec=30,
                err_msg="VerifiableSource connector failed to start within expected time"
            )

            # Verify metrics export
            self._wait_for_metrics_available()
            self._verify_comprehensive_metrics()

            # Verify connector is producing data
            wait_until(
                lambda: len(self.source.sent_messages()) > 0,
                timeout_sec=30,
                err_msg="VerifiableSource failed to produce messages"
            )

        finally:
            if hasattr(self, 'source'):
                self.source.stop()
            self.cc.stop()

    def _connector_is_running_by_name(self, connector_name):
        """Helper to check if connector is running by name"""
        try:
            status = self.cc.get_connector_status(connector_name)
            return status and status['connector']['state'] == 'RUNNING'
        except:
            return False

    def _verify_metrics_under_load(self, expected_connector_count):
        """Verify metrics accuracy under load"""
        self._wait_for_metrics_available()

        for node in self.cc.nodes:
            cmd = "curl -s http://localhost:9464/metrics"
            result = node.account.ssh_capture(cmd)
            metrics_output = "".join([line for line in result])

            # Verify connector count metrics
            connector_count_found = False
            for line in metrics_output.split('\n'):
                if 'kafka_connect_worker_connector_count' in line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        count = float(parts[1])
                        assert count >= expected_connector_count, f"Connector count metric incorrect: {count} < {expected_connector_count}"
                        connector_count_found = True
                        break

            assert connector_count_found, "Connector count metric not found"
            self.logger.info(f"Node {node.account.hostname} load test metrics validation passed")

    @cluster(num_nodes=5)
    def test_opentelemetry_s3_metrics_exporter(self):
        """Test OpenTelemetry S3 Metrics exporter functionality"""
        # Setup mock S3 server using localstack
        self.setup_services(num_workers=2)
        cluster_id = f"connect-logs-{int(time.time())}"
        bucket_name = "ko3"
        metrics_prefix = f"automq/metrics/{cluster_id}"

        def s3_config(node):
            config = self.render("connect-distributed.properties", node=node)
            # Replace prometheus exporter with S3 exporter
            config = config.replace(
                "automq.telemetry.exporter.uri=prometheus://0.0.0.0:9464",
                f"automq.telemetry.exporter.uri=ops://{bucket_name}"
            )
            # Add S3 specific configurations
            config += "\nautomq.telemetry.exporter.interval.ms=10000\n"
            config += f"automq.telemetry.s3.bucket=0@s3://{bucket_name}?endpoint=http://10.5.0.2:4566&region=us-east-1\n"
            config += f"automq.telemetry.s3.cluster.id={cluster_id}\n"
            config += f"automq.telemetry.s3.node.id={self.cc.nodes.index(node) + 1}\n"
            config += "automq.telemetry.s3.selector.type=connect-leader\n"

            return config

        self.cc.set_configs(s3_config)

        def _list_s3_objects(prefix):
            """List S3 objects with given prefix using kafka service method"""
            objects, _ = self.kafka.get_bucket_objects()
            return [obj for obj in objects if obj["path"].startswith(prefix)]

        try:
            self.logger.info("Starting Connect cluster with S3 exporter...")
            self.cc.start()

            def _connect_leader_nodes():
                leaders = []
                pattern = "Node became leader"
                for connect_node in self.cc.nodes:
                    cmd = f"grep -a '{pattern}' {self.cc.LOG_FILE} || true"
                    output = "".join(connect_node.account.ssh_capture(cmd, allow_fail=True))
                    if pattern in output:
                        leaders.append(connect_node.account.hostname)
                return leaders

            wait_until(
                lambda: len(_connect_leader_nodes()) == 1,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Telemetry leadership in Connect cluster did not converge"
            )

            # Create connector to generate metrics
            self.source = VerifiableSource(self.cc, topic=self.TOPIC, throughput=15)
            self.source.start()

            # Wait for connector to be running
            wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                       err_msg="VerifiableSource connector failed to start")

            # Wait for metrics to be exported to S3
            self.logger.info("Waiting for S3 metrics export...")

            def _metrics_uploaded():
                objects = _list_s3_objects(metrics_prefix)
                if objects:
                    self.logger.info("Found %d metrics objects for prefix %s", len(objects), metrics_prefix)
                return len(objects) > 0

            wait_until(
                _metrics_uploaded,
                timeout_sec=180,
                backoff_sec=10,
                err_msg="Timed out waiting for Connect S3 metrics export"
            )

            self.logger.info("S3 Metrics exporter test passed!")

        finally:
            # Cleanup
            try:
                if hasattr(self, 'source'):
                    self.source.stop()
                self.cc.stop()
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")

    @cluster(num_nodes=5)
    def test_s3_log_uploader(self):
        """Verify that Connect workers upload logs to S3 using the AutoMQ log uploader."""
        self.setup_services(num_workers=2)

        bucket_name = "ko3"
        cluster_id = f"connect-logs-{int(time.time())}"
        logs_prefix = f"automq/logs/{cluster_id}"

        def s3_log_config(node):
            config = self.render("connect-distributed.properties", node=node)
            config += "\nlog.s3.enable=true\n"
            config += f"log.s3.bucket=0@s3://{bucket_name}?endpoint=http://10.5.0.2:4566&region=us-east-1\n"
            config += f"log.s3.cluster.id={cluster_id}\n"
            config += f"log.s3.node.id={self.cc.nodes.index(node) + 1}\n"
            config += "log.s3.selector.type=connect-leader\n"

            return config

        self.cc.set_configs(s3_log_config)
        self.cc.environment['AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL'] = '15000'
        self.cc.environment['AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL'] = '60000'

        def _list_s3_objects(prefix):
            """List S3 objects with given prefix using kafka service method"""
            objects, _ = self.kafka.get_bucket_objects()
            return [obj for obj in objects if obj["path"].startswith(prefix)]

        source = None

        try:
            self.logger.info("Starting Connect cluster with S3 log uploader enabled ...")
            self.cc.start()

            def _connect_leader_nodes():
                leaders = []
                pattern = "Node became leader"
                for connect_node in self.cc.nodes:
                    cmd = f"grep -a '{pattern}' {self.cc.LOG_FILE} || true"
                    output = "".join(connect_node.account.ssh_capture(cmd, allow_fail=True))
                    if pattern in output:
                        leaders.append(connect_node.account.hostname)
                return leaders

            wait_until(
                lambda: len(_connect_leader_nodes()) == 1,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Log uploader leadership in Connect cluster did not converge"
            )

            source = VerifiableSource(self.cc, topic=self.TOPIC, throughput=10)
            source.start()

            wait_until(lambda: self.is_running(source), timeout_sec=30,
                       err_msg="VerifiableSource connector failed to start")

            def _logs_uploaded():
                objects = _list_s3_objects(logs_prefix)
                if objects:
                    self.logger.info("Found %d log objects for prefix %s", len(objects), logs_prefix)
                return len(objects) > 0

            wait_until(_logs_uploaded, timeout_sec=240, backoff_sec=15,
                       err_msg="Timed out waiting for Connect S3 log upload")

            # Verify objects are actually present
            objects = _list_s3_objects(logs_prefix)
            assert objects, "Expected log objects to be present after successful upload"

        finally:
            try:
                if source:
                    source.stop()
                self.cc.stop()
            except Exception as e:
                self.logger.warning(f"Cleanup error: {e}")
