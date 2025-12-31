"""
Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
"""

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.automq.automq_e2e_util import TOPIC, run_simple_load, RECORD_NUM
from kafkatest.services.kafka import KafkaService


class OTLPMetricsExporterTest(Test):
    """
    Test OTLP HTTP metrics exporter functionality.
    """

    def __init__(self, test_context):
        super(OTLPMetricsExporterTest, self).__init__(test_context)
        self.context = test_context
        self.topic = TOPIC

    def create_kafka_with_otlp(self, num_nodes=1, partition=1):
        """
        Create and configure Kafka service with OTLP metrics exporter enabled.
        """
        server_prop_overrides = [
            ['s3.telemetry.metrics.exporter.uri', 'http://localhost:9090/opentelemetry/v1/metrics'],
            ['s3.telemetry.metrics.exporter.report.interval.ms', '30000'],
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
        """
        self.logger.info("Testing broker startup with OTLP HTTP metrics exporter enabled")
        
        self.create_kafka_with_otlp(num_nodes=1, partition=1)
        
        self.logger.info("Starting Kafka broker with OTLP exporter enabled...")
        self.kafka.start()
        
        self.logger.info("Verifying broker is running...")
        wait_until(
            lambda: self.kafka.pids(self.kafka.nodes[0]),
            timeout_sec=30,
            err_msg="Broker failed to start within 30 seconds"
        )
        
        self.logger.info("Checking logs for OTLP initialization...")
        node = self.kafka.nodes[0]
        
        abstract_method_error_found = False
        for line in node.account.ssh_capture('grep -i "AbstractMethodError" ' + self.kafka.STDOUT_STDERR_CAPTURE + ' || true'):
            if "AbstractMethodError" in line:
                abstract_method_error_found = True
                self.logger.error("Found AbstractMethodError in logs: " + line)
        
        assert not abstract_method_error_found, "Broker startup failed with AbstractMethodError"
        
        otlp_initialized = False
        for line in node.account.ssh_capture('grep "OTLPMetricsExporter initialized" ' + self.kafka.STDOUT_STDERR_CAPTURE + ' || true'):
            if "OTLPMetricsExporter initialized" in line:
                otlp_initialized = True
                self.logger.info("OTLP exporter initialized successfully")
        
        assert otlp_initialized, "OTLP metrics exporter was not initialized"
        
        self.logger.info("Testing basic produce/consume functionality...")
        run_simple_load(
            test_context=self.context,
            kafka=self.kafka,
            logger=self.logger,
            topic=self.topic,
            num_records=1000,
            throughput=-1
        )
        
        self.logger.info("Test passed")

    @cluster(num_nodes=4)
    def test_otlp_exporter_with_load(self):
        """
        Test OTLP metrics exporter under load.
        """
        self.logger.info("Testing OTLP exporter stability under load")
        
        self.create_kafka_with_otlp(num_nodes=1, partition=3)
        self.kafka.start()
        
        wait_until(
            lambda: self.kafka.pids(self.kafka.nodes[0]),
            timeout_sec=30,
            err_msg="Broker failed to start"
        )
        
        self.logger.info("Running load test...")
        run_simple_load(
            test_context=self.context,
            kafka=self.kafka,
            logger=self.logger,
            topic=self.topic,
            num_records=RECORD_NUM,
            throughput=-1
        )
        
        assert self.kafka.pids(self.kafka.nodes[0]), "Broker crashed during load test"
        
        self.logger.info("Test passed")
