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

import subprocess
import time

from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService


class AutoMQBrokerTelemetryTest(Test):
    """End-to-end validation for AutoMQ telemetry and log uploader integration in the broker."""

    TOPIC = "automq-telemetry-topic"

    def __init__(self, test_context):
        super(AutoMQBrokerTelemetryTest, self).__init__(test_context)
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

    def _metrics_ready(self, node, port):
        try:
            cmd = f"curl -sf http://localhost:{port}/metrics"
            output = "".join(list(node.account.ssh_capture(cmd, allow_fail=True)))
            return bool(output.strip())
        except Exception:
            return False

    def _wait_for_metrics_available(self, port=9464, timeout_sec=90):
        for node in self.kafka.nodes:
            wait_until(
                lambda n=node: self._metrics_ready(n, port),
                timeout_sec=timeout_sec,
                backoff_sec=5,
                err_msg=f"Metrics endpoint not available on {node.account.hostname}",
            )

    def _fetch_metrics(self, node, port=9464):
        cmd = f"curl -sf http://localhost:{port}/metrics"
        return "".join(list(node.account.ssh_capture(cmd, allow_fail=True)))

    def _assert_prometheus_metrics(self, metrics_output, expected_labels=None):
        assert metrics_output.strip(), "Metrics endpoint returned no data"

        metric_lines = [
            line for line in metrics_output.splitlines()
            if line.strip() and not line.startswith('#')
        ]
        assert metric_lines, "No metric datapoints found in Prometheus output"

        kafka_lines = [line for line in metric_lines if 'kafka_' in line or 'automq' in line]
        assert kafka_lines, "Expected broker metrics not present in Prometheus output"

        if expected_labels:
            for label in expected_labels:
                assert label in metrics_output, f"Expected label '{label}' absent from metrics output"

        if "# HELP" not in metrics_output and "# TYPE" not in metrics_output:
            self.logger.warning("Metrics output missing HELP/TYPE comments – format may not follow Prometheus conventions")

    def _list_s3_objects(self, prefix):
        objects, _ = self.kafka.get_bucket_objects()
        return [obj for obj in objects if obj["path"].startswith(prefix)]

    def _clear_s3_prefix(self, bucket, prefix):
        cmd = f"aws s3 rm s3://{bucket}/{prefix} --recursive --endpoint=http://10.5.0.2:4566"
        ret, out = subprocess.getstatusoutput(cmd)
        if ret != 0:
            self.logger.info("Ignoring cleanup error for prefix %s: %s", prefix, out)

    def _check_port_listening(self, node, port):
        """Check if a port is listening on the given node."""
        try:
            result = list(node.account.ssh_capture(f"netstat -ln | grep :{port}", allow_fail=True))
            return len(result) > 0
        except Exception:
            return False

    def _extract_metric_samples(self, metrics_output, metric_name):
        samples = []
        for line in metrics_output.splitlines():
            if line.startswith(metric_name):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        samples.append(float(parts[-1]))
                    except ValueError:
                        continue
        return samples

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    @cluster(num_nodes=4)
    def test_prometheus_metrics_exporter(self):
        """Verify that the broker exposes Prometheus metrics via the AutoMQ OpenTelemetry module."""
        cluster_label = f"kafka-core-prom-{int(time.time())}"
        server_overrides = [
            ["s3.telemetry.metrics.exporter.uri", "prometheus://0.0.0.0:9464"],
            ["s3.telemetry.exporter.report.interval.ms", "10000"],
            ["s3.telemetry.metrics.base.labels", "component=broker"]
        ]

        self._start_kafka(server_overrides=server_overrides)

        try:
            self._produce_messages(max_messages=200)
            self._wait_for_metrics_available()

            for node in self.kafka.nodes:
                output = self._fetch_metrics(node)
                self._assert_prometheus_metrics(
                    output,
                    expected_labels=['instance="']
                )
        finally:
            self._stop_kafka()

    @cluster(num_nodes=4)
    def test_s3_metrics_exporter(self):
        """Verify that broker metrics are exported to S3 via the AutoMQ telemetry module."""
        bucket_name = "ko3"
        metrics_prefix = f"automq/metrics"

        self._clear_s3_prefix(bucket_name, metrics_prefix)

        server_overrides = [
            ["s3.telemetry.metrics.exporter.uri", f"ops://{bucket_name}"],
            ["s3.telemetry.ops.enabled", "true"],
            ["s3.ops.buckets", f"0@s3://{bucket_name}?endpoint=http://10.5.0.2:4566&region=us-east-1"],
        ]

        original_num_brokers = self.num_brokers
        node_count = max(2, self.num_brokers)
        self.num_brokers = node_count

        try:
            self._start_kafka(server_overrides=server_overrides)

            self._produce_messages(max_messages=200)

            def _metrics_uploaded():
                objects = self._list_s3_objects(metrics_prefix)
                if objects:
                    self.logger.info("Found %d metrics objects for prefix %s", len(objects), metrics_prefix)
                return len(objects) > 0

            wait_until(
                _metrics_uploaded,
                timeout_sec=180,
                backoff_sec=10,
                err_msg="Timed out waiting for S3 metrics export"
            )

        finally:
            self.num_brokers = original_num_brokers
            self._stop_kafka()

    @cluster(num_nodes=4)
    def test_s3_log_uploader(self):
        """Verify that broker logs are uploaded to S3 via the AutoMQ log uploader module."""
        bucket_name = "ko3"
        logs_prefix = f"automq/logs"
        
        self._clear_s3_prefix(bucket_name, logs_prefix)

        server_overrides = [
            ["s3.telemetry.ops.enabled", "true"],
            ["s3.ops.buckets", f"0@s3://{bucket_name}?endpoint=http://10.5.0.2:4566&region=us-east-1"],
        ]

        extra_env = [
            "AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL=15000",
            "AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL=60000"
        ]

        original_num_brokers = self.num_brokers
        node_count = max(2, self.num_brokers)
        self.num_brokers = node_count

        try:
            self._start_kafka(server_overrides=server_overrides, extra_env=extra_env)

            self._produce_messages(max_messages=300)

            def _logs_uploaded():
                objects = self._list_s3_objects(logs_prefix)
                if objects:
                    self.logger.info("Found %d log objects for prefix %s", len(objects), logs_prefix)
                return len(objects) > 0

            wait_until(
                _logs_uploaded,
                timeout_sec=240,
                backoff_sec=15,
                err_msg="Timed out waiting for S3 log upload"
            )

        finally:
            self.num_brokers = original_num_brokers
            self._stop_kafka()
