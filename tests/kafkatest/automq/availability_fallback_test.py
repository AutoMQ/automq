"""
Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
"""

import time

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService
from kafkatest.services.security.security_config import SecurityConfig


class AvailabilityFallbackTest(Test):
    """
    End-to-end validation for AutoMQ availability fallback signal publication and dry-run planning.
    """

    TOPIC = "availability-fallback-e2e"
    S3_IP = "10.5.0.2"
    S3_PORT = 4566

    def __init__(self, test_context):
        super(AvailabilityFallbackTest, self).__init__(test_context)
        self.kafka = None

    def _start_kafka(self):
        server_overrides = [
            ["automq.auto.fallback.enabled", "true"],
            ["automq.auto.fallback.action.allowlist", "PARTITION_REASSIGNMENT"],
            ["automq.auto.fallback.append.stuck.threshold.ms", "1000"],
            ["automq.auto.fallback.cold.read.stuck.threshold.ms", "1000"],
            ["automq.auto.fallback.attribution.consecutive.threshold", "1"],
            ["automq.auto.fallback.controller.reconcile.interval.ms", "3000"],
            ["automq.auto.fallback.signal.write.interval.ms", "1000"],
            ["automq.auto.fallback.signal.stale.ms", "10000"],
            ["automq.auto.fallback.node.cooldown.ms", "5000"],
            ["automq.auto.fallback.partition.log.cooldown.ms", "5000"],
            ["automq.auto.fallback.response.retention.ms", "30000"],
            ["automq.auto.fallback.action.cleanup.grace.ms", "30000"],
            ["automq.auto.fallback.cleanup.interval.ms", "5000"],
        ]
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=3,
            zk=None,
            security_protocol=SecurityConfig.PLAINTEXT,
            topics={
                self.TOPIC: {
                    "partitions": 1,
                    "replication-factor": 1,
                    "configs": {"min.insync.replicas": 1},
                }
            },
            server_prop_overrides=server_overrides,
        )
        self.kafka.start(timeout_sec=120)

    def _reject_s3_traffic(self, node):
        node.account.ssh(
            "sudo iptables -I OUTPUT -p tcp -d %s --dport %d -j REJECT" % (self.S3_IP, self.S3_PORT)
        )

    def _reset_s3_traffic(self, node):
        node.account.ssh(
            "while sudo iptables -D OUTPUT -p tcp -d %s --dport %d -j REJECT 2>/dev/null; do true; done"
            % (self.S3_IP, self.S3_PORT),
            allow_fail=True,
        )

    def _run_bounded_perf_producer(self, node, timeout_sec=45):
        cmd = (
            "timeout %d %s --topic %s --num-records 100000 --record-size 1024 --throughput -1 "
            "--producer-props bootstrap.servers=%s acks=all request.timeout.ms=30000 "
            "delivery.timeout.ms=60000 linger.ms=0 batch.size=16384 || true"
        ) % (
            timeout_sec,
            self.kafka.path.script("kafka-producer-perf-test.sh", node),
            self.TOPIC,
            self.kafka.bootstrap_servers(),
        )
        node.account.ssh(cmd, allow_fail=True)

    def _grep_logs(self, pattern):
        lines = []
        cmd = (
            "grep -E '%s' /mnt/kafka/kafka-operational-logs/info/server.log 2>/dev/null || true; "
            "grep -E '%s' /mnt/kafka/kafka-operational-logs/debug/server.log 2>/dev/null || true"
        ) % (pattern, pattern)
        for node in self.kafka.nodes:
            lines.extend([line.strip() for line in node.account.ssh_capture(cmd, allow_fail=True) if line.strip()])
        return lines

    def _wait_for_fallback_logs(self):
        found = []

        def check():
            lines = self._grep_logs("APPEND_STUCK|AVAILABILITY_FALLBACK|DRY_RUN|BLOCKED")
            for line in lines:
                self.logger.info("availability fallback log: %s", line)
            has_append_stuck = any("APPEND_STUCK" in line for line in lines)
            has_controller_decision = any(
                "Availability fallback decision" in line or "type=DRY_RUN" in line or "type=BLOCKED" in line
                for line in lines
            )
            if has_append_stuck and has_controller_decision:
                found[:] = lines
                return True
            return False

        wait_until(
            check,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Availability fallback APPEND_STUCK signal and controller decision logs were not observed",
        )
        return found

    @cluster(num_nodes=3)
    def test_append_stuck_publishes_signal_and_dry_run_decision(self):
        """
        Isolate the leader from LocalStack S3 during sustained produce and verify fallback logs.
        """
        self._start_kafka()
        leader = self.kafka.leader(self.TOPIC, partition=0)
        producer_node = self.kafka.nodes[0]
        self.logger.info("Topic %s leader is %s", self.TOPIC, leader.name)

        try:
            self._reject_s3_traffic(leader)
            time.sleep(2)
            self._run_bounded_perf_producer(producer_node)
            logs = self._wait_for_fallback_logs()
            assert any("APPEND_STUCK" in line for line in logs), "Expected APPEND_STUCK signal log"
            assert any("NODE_EXIT" in line and "DRY_RUN" in line for line in logs), \
                "Expected NODE_EXIT dry-run decision because allowlist excludes NODE_EXIT"
        finally:
            self._reset_s3_traffic(leader)
            if self.kafka:
                self.kafka.stop()
