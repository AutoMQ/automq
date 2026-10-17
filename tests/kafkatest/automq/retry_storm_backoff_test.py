"""
Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
"""

import re

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.automq.automq_e2e_util import S3_WAL
from kafkatest.services.kafka import KafkaService


class RetryStormBackoffTest(Test):
    """
    End-to-end validation for retry storm delayed response behavior.
    """

    PROBE_CLASS = "org.apache.kafka.tools.automq.RetryStormBackoffProbe"

    def __init__(self, test_context):
        super(RetryStormBackoffTest, self).__init__(test_context)
        self.kafka = None

    def create_kafka(self):
        """
        Create a small AutoMQ Kafka cluster with retry storm backoff using its default disabled state.
        """
        log_size = 256 * 1024 * 1024
        server_prop_overrides = [
            ["s3.wal.cache.size", str(log_size)],
            ["s3.wal.capacity", str(log_size)],
            ["s3.wal.upload.threshold", str(log_size // 4)],
            ["s3.block.cache.size", str(log_size)],
            ["s3.wal.path", S3_WAL],
        ]
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=3,
            zk=None,
            kafka_heap_opts="-Xmx1024m -Xms1024m",
            server_prop_overrides=server_prop_overrides,
            topics={},
        )

    def alter_retry_storm_config(self, enabled, max_delay_ms=None):
        """
        Dynamically alter retry storm config on every broker so clients can hit any connection target.
        """
        config = "automq.retry.storm.backoff.enabled=%s" % str(enabled).lower()
        if max_delay_ms is not None:
            config += ",automq.retry.storm.backoff.max.delay.ms=%d" % max_delay_ms

        node = self.kafka.nodes[0]
        for broker in self.kafka.nodes:
            cmd = "%s --alter --add-config %s --entity-type brokers --entity-name %s" % (
                self.kafka.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection=False),
                config,
                self.kafka.idx(broker),
            )
            node.account.ssh(cmd)

    def run_probe(self, topic, iterations=8):
        """
        Run a Java AdminClient probe that keeps one connection while repeatedly describing a missing topic.
        """
        node = self.kafka.nodes[0]
        bootstrap_servers = self.kafka.bootstrap_servers()
        run_cmd = (
            "KAFKA_HEAP_OPTS='-Xms64m -Xmx128m' KAFKA_JVM_PERFORMANCE_OPTS='' "
            "%s %s '%s' '%s' %d" %
            (
                self.kafka.path.script("kafka-run-class.sh", node),
                self.PROBE_CLASS,
                bootstrap_servers,
                topic,
                iterations,
            )
        )
        output = list(node.account.ssh_capture(run_cmd))
        return self.parse_elapsed_ms(output)

    def parse_elapsed_ms(self, output):
        """
        Parse probe stdout into per-request elapsed milliseconds.
        """
        elapsed = []
        for line in output:
            match = re.search(r"elapsedMs=(\d+)", line)
            if match:
                elapsed.append(int(match.group(1)))
        if not elapsed:
            raise AssertionError("Probe did not return elapsed timings. Output: %s" % "".join(output))
        self.logger.info("Retry storm probe elapsed times: %s", elapsed)
        return elapsed

    def assert_no_protective_delay(self, elapsed):
        """
        Assert that protective-threshold requests are not delayed by retry storm backoff.
        """
        assert max(elapsed[5:]) < 300, "Expected no retry storm delay, but got timings %s" % elapsed

    def assert_protective_delay(self, elapsed, min_delay_ms):
        """
        Assert that the sixth and later same-connection missing-topic metadata requests are delayed.
        """
        assert max(elapsed[1:5]) < 300, "Expected requests two through five to return quickly, but got %s" % elapsed
        assert min(elapsed[5:]) >= min_delay_ms, "Expected protective delay from sixth request, but got %s" % elapsed

    @cluster(num_nodes=3)
    def test_metadata_protective_backoff_dynamic_config(self):
        """
        Verify default-disabled, dynamic-enable, and dynamic-disable behavior for Metadata protective backoff.
        """
        self.create_kafka()
        self.kafka.start()

        disabled_elapsed = self.run_probe("retry-storm-default-disabled-topic", iterations=8)
        self.assert_no_protective_delay(disabled_elapsed)

        self.alter_retry_storm_config(enabled=True, max_delay_ms=500)
        enabled_elapsed = self.run_probe("retry-storm-enabled-topic", iterations=8)
        self.assert_protective_delay(enabled_elapsed, min_delay_ms=350)

        self.alter_retry_storm_config(enabled=False)
        disabled_again_elapsed = self.run_probe("retry-storm-disabled-again-topic", iterations=8)
        self.assert_no_protective_delay(disabled_again_elapsed)
