"""
Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.

Test for OTLP HTTP metrics exporter startup issue (Issue #3111).
This test verifies that the broker can start successfully when OTLP HTTP metrics exporter is enabled.
"""

import time
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.automq.automq_e2e_util import (
    TOPIC, run_simple_load, RECORD_NUM
)
from kafkatest.services.kafka import KafkaService


class OTLPMetricsExporterTest(Test):
    """
    Test OTLP HTTP metrics exporter functionality.
    
    This test verifies the fix for issue #3111 where enabling OTLP HTTP metrics exporter
    caused broker startup failure due to missing HTTP sender implementation.
    """

    def __init__(self, test_context):
        super(OTLPMetricsExporterTest, self).__init__(test_context)
        self.context = test_context
        self.topic = TOPIC

    def create_kafka_with_otlp(self, num_nodes=1, partition=1, otlp_endpoint="http://localhost:9090/opentelemetry/v1/metrics"):
        """
        Create and configure Kafka service with OTLP metrics exporter enabled.

        Args:
            num_nodes: Number of Kafka nodes.
            partition: Number of partitions.
            otlp_endpoint: OTLP HTTP endpoint for metrics export.
        """
        server_prop_overrides = [
            # Enable OTLP metrics exporter
            ['s3.telemetry.metrics.exporter.uri', otlp_endpoint],
            ['s3.telemetry.metrics.exporter.report.interval.ms', '30000'],
            # Basic S3 stream configurations
            ['s3.wal.cache.size', str(256 * 1024 * 1024)],
            ['s3.wal.capacity', str(256 * 1024 * 1024)],
            ['s3.block.cache.size', str(256 * 1024 * 1024)],
            ['autobalancer.controller.enable', 'false'],
        ]

        self.kafka = KafkaService(
            self.context,
            num_nodes=num_nodes,
            zk=None,
            kafka_heap_opts="-Xmx2048m -Xms2048m",
            server_prop_overrides=server_prop_overrides,
            topics={
                self.topic: {
                    'partitions': partition,
                    'replication-factor': 1,
                    'configs': {
                        'min.insync.replicas': 1
                    }
                }
            }
        )

    @cluster(num_nodes=4)
    def test_broker_startup_with_otlp_enabled(self):
        """
        Test that broker starts successfully with OTLP HTTP metrics exporter enabled.
        
        This test verifies the fix for issue #3111:
        - Broker should start without AbstractMethodError
        - Broker should not crash during OTLP exporter initialization
        - Basic produce/consume operations should work normally
        """
        self.logger.info("Testing broker startup with OTLP HTTP metrics exporter enabled")
        
        # Create Kafka with OTLP enabled
        # Note: We use a dummy endpoint since we're testing startup, not actual metrics export
        self.create_kafka_with_otlp(
            num_nodes=1,
            partition=1,
            otlp_endpoint="http://localhost:9090/opentelemetry/v1/metrics"
        )
        
        # Start Kafka - this should succeed without AbstractMethodError
        self.logger.info("Starting Kafka broker with OTLP exporter enabled...")
        self.kafka.start()
        
        # Verify broker is running
        self.logger.info("Verifying broker is running...")
        wait_until(
            lambda: self.kafka.pids(self.kafka.nodes[0]),
            timeout_sec=30,
            err_msg="Broker failed to start within 30 seconds"
        )
        
        # Check logs for successful OTLP initialization (no AbstractMethodError)
        self.logger.info("Checking logs for OTLP initialization...")
        node = self.kafka.nodes[0]
        
        # Check that we don't have the AbstractMethodError
        abstract_method_error_found = False
        for line in node.account.ssh_capture(f'grep -i "AbstractMethodError" {self.kafka.STDOUT_STDERR_CAPTURE} || true'):
            if "AbstractMethodError" in line:
                abstract_method_error_found = True
                self.logger.error(f"Found AbstractMethodError in logs: {line}")
        
        assert not abstract_method_error_found, "Broker startup failed with AbstractMethodError - OTLP exporter issue not fixed"
        
        # Check for successful OTLP initialization
        otlp_initialized = False
        for line in node.account.ssh_capture(f'grep "OTLPMetricsExporter initialized" {self.kafka.STDOUT_STDERR_CAPTURE} || true'):
            if "OTLPMetricsExporter initialized" in line:
                otlp_initialized = True
                self.logger.info(f"OTLP exporter initialized successfully: {line}")
        
        assert otlp_initialized, "OTLP metrics exporter was not initialized"
        
        # Verify basic produce/consume functionality works
        self.logger.info("Testing basic produce/consume functionality...")
        run_simple_load(
            test_context=self.context,
            kafka=self.kafka,
            logger=self.logger,
            topic=self.topic,
            num_records=1000,  # Use smaller number for quick verification
            throughput=-1
        )
        
        self.logger.info("Test passed: Broker started successfully with OTLP exporter enabled and basic operations work")

    @cluster(num_nodes=4)
    def test_otlp_exporter_with_load(self):
        """
        Test OTLP metrics exporter under load.
        
        Verifies that the broker remains stable with OTLP exporter enabled
        while handling production workload.
        """
        self.logger.info("Testing OTLP exporter stability under load")
        
        self.create_kafka_with_otlp(num_nodes=1, partition=3)
        self.kafka.start()
        
        # Wait for broker to be fully ready
        wait_until(
            lambda: self.kafka.pids(self.kafka.nodes[0]),
            timeout_sec=30,
            err_msg="Broker failed to start"
        )
        
        # Run load test
        self.logger.info("Running load test with OTLP exporter enabled...")
        run_simple_load(
            test_context=self.context,
            kafka=self.kafka,
            logger=self.logger,
            topic=self.topic,
            num_records=RECORD_NUM,
            throughput=-1
        )
        
        # Verify broker is still running after load
        assert self.kafka.pids(self.kafka.nodes[0]), "Broker crashed during load test"
        
        self.logger.info("Test passed: Broker remained stable under load with OTLP exporter enabled")
