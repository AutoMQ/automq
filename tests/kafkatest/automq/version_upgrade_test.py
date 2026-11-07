"""
Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
"""
import time

from ducktape.mark import parametrize, ignore
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.automq.automq_e2e_util import (TOPIC, FILE_WAL, S3_WAL, formatted_time)
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import AUTOMQ_LATEST_1_1, DEV_BRANCH, KafkaVersion, LATEST_STABLE_METADATA_VERSION, \
    LATEST_STABLE_AUTOMQ_VERSION


class TestUpgrade(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(TestUpgrade, self).__init__(test_context)
        self.context = test_context

    def setUp(self):
        self.topic = TOPIC
        self.partitions = 1
        self.replication_factor = 1

        # Producer and consumer
        self.producer_throughput = 10
        self.num_producers = 1
        self.num_consumers = 1

    def create_kafka(self, num_nodes=3, broker_wal='s3', env=None, project_name=None, version=DEV_BRANCH):
        """
        Create and configure Kafka service.

        Args:
            :param version: KafkaVersion
            :param replication_factor: replication_factor
            :param project_name: automq or kafka
            :param env: additional environmental variables
            :param num_nodes: Number of Kafka nodes.
            :param partition: Number of partitions.
            :param broker_wal: wal
        """
        server_prop_overrides = [
            ['autobalancer.controller.enable', 'false'],
            ['s3.wal.path', FILE_WAL if broker_wal == 'file' else S3_WAL],
        ]
        extra_env = []
        if env:
            extra_env.extend(env)
        self.kafka = KafkaService(self.context, num_nodes=num_nodes, zk=None,
                                  server_prop_overrides=server_prop_overrides,
                                  extra_env=extra_env,
                                  topics={
                                      self.topic: {
                                          'partitions': self.partitions,
                                          'replication-factor': self.replication_factor,
                                          'configs': {
                                              'min.insync.replicas': 1
                                          }
                                      }
                                  },
                                  project_name=project_name,
                                  version=version,
                                  kafka_heap_opts="-Xmx1536m -Xms1536m",
                                  )

    def perform_upgrade(self):
        self.logger.info("First pass bounce - rolling upgrade")
        time.sleep(10)
        for node in self.kafka.nodes:
            self.logger.info(f'stop {node} time is {formatted_time()}')
            self.kafka.stop_node(node)
            node.version = DEV_BRANCH
            self.kafka.start_node(node)
            time.sleep(5)
        self.kafka.run_features_command(op='upgrade', key='metadata', new_version=LATEST_STABLE_METADATA_VERSION)
        self.kafka.run_features_command(op='upgrade', key='feature', new_version=f'automq.version={LATEST_STABLE_AUTOMQ_VERSION}')

    # FIXME
    @ignore
    @cluster(num_nodes=5)
    @parametrize(from_kafka_version=str(AUTOMQ_LATEST_1_1))
    def test_upgrade(self, from_kafka_version):
        self.create_kafka(project_name='kafka', version=KafkaVersion(from_kafka_version))
        self.kafka.start()

        self.logger.info(f'TP_INFO {self.topic}-0 isr_idx_list is: {self.kafka.isr_idx_list(self.topic, 0)}')
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           version=DEV_BRANCH)

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, new_consumer=True, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=DEV_BRANCH)

        self.run_produce_consume_validate(core_test_action=lambda: self.perform_upgrade())
