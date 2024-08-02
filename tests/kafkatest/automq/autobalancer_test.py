# Copyright 2024, AutoMQ HK Limited.
#
# The use of this file is governed by the Business Source License,
# as detailed in the file "/LICENSE.S3Stream" included in this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.mark import matrix
from kafkatest.automq.automq_e2e_util import (FILE_WAL, S3_WAL, run_simple_load, TOPIC, append_info)
from kafkatest.services.kafka import KafkaService

# Configuration constants for the AutoBalancer
REPORT_INTERVAL = 'autobalancer.reporter.metrics.reporting.interval.ms'
DETECT_INTERVAL = 'autobalancer.controller.anomaly.detect.interval.ms'
ENABLE = 'autobalancer.controller.enable'
IN_AVG_DEVIATION = 'autobalancer.controller.network.in.distribution.detect.avg.deviation'
OUT_AVG_DEVIATION = 'autobalancer.controller.network.out.distribution.detect.avg.deviation'
OUT_THRESHOLD = 'autobalancer.controller.network.out.usage.distribution.detect.threshold'
IN_THRESHOLD = 'autobalancer.controller.network.in.usage.distribution.detect.threshold'
GOALS = 'autobalancer.controller.goals'
EXCLUDE_TOPIC = 'autobalancer.controller.exclude.topics'
EXCLUDE_BROKER = 'autobalancer.controller.exclude.broker.ids'
METRIC_REPORTERS = 'metric.reporters'


def get_partition_count_per_broker(partition_data, exclude=None):
    """
    Get the count of partitions per broker.

    :param partition_data: Data of partitions
    :param exclude: Set of broker IDs to exclude from the count
    :return: Dictionary with broker IDs as keys and partition counts as values
    """
    if exclude is None:
        exclude = set()
    broker_replicas_count = {}

    partitions = partition_data.get('partitions', [])
    for partition in partitions:
        replicas = partition.get('replicas', [])
        for broker_id in replicas:
            broker_id = str(broker_id)
            if broker_id in exclude:
                continue
            if broker_id not in broker_replicas_count:
                broker_replicas_count[broker_id] = 0
            broker_replicas_count[broker_id] += 1

    return broker_replicas_count


def check_partition_replicas(partition_data, exclude=None):
    """
    Check if the partition replicas are evenly distributed among brokers.

    :param partition_data: Data of partitions
    :param exclude: Set of broker IDs to exclude from the check
    :return: Tuple of success status and message
    """
    broker_replicas_count = get_partition_count_per_broker(partition_data, exclude)
    replica_counts = list(broker_replicas_count.values())

    success = True
    msg = ''
    if len(set(replica_counts)) != 1:
        success = False
        msg = "Brokers have different numbers of partition replicas: " + str(broker_replicas_count)

    return success, msg


def check_partition_eq(topic_info1, topic_info2):
    success, msg = True, ''
    success = str(topic_info1) == str(topic_info2)
    topic_name = topic_info1.get('partitions')[0]['topic']
    if not success:
        msg += f"Topic {topic_name} was modified despite being excluded from AutoBalancer. Before: {topic_info1}, After: {topic_info2}"
    return success, msg


class AutoBalancerTest(Test):
    """
    Test class for AutoBalancer functionality
    """

    def __init__(self, test_context):
        super(AutoBalancerTest, self).__init__(test_context)
        self.context = test_context
        self.start = False
        self.topic = TOPIC
        self.avg_deviation = 0.2
        self.maximum_broker_deviation_percentage = 0.15

    def create_kafka(self, num_nodes=1, partition=1, exclude_broker=None, exclude_topic=None, replica_assignment=None, wal='file'):
        """
        Create and configure a Kafka cluster for testing.

        :param num_nodes: Number of Kafka nodes
        :param partition: Number of partitions
        :param exclude_broker: Brokers to exclude from AutoBalancer
        :param exclude_topic: Topics to exclude from AutoBalancer
        :param replica_assignment: Replica assignment for partitions
        """
        log_size = 256 * 1024 * 1024
        block_size = 256 * 1024 * 1024
        threshold = 512 * 1024
        server_prop_overrides = [
            ['s3.wal.cache.size', str(log_size)],
            ['s3.wal.capacity', str(log_size)],
            ['s3.wal.upload.threshold', str(log_size // 4)],
            ['s3.block.cache.size', str(block_size)],
            [ENABLE, 'true'],
            [IN_AVG_DEVIATION, str(self.avg_deviation)],
            [OUT_AVG_DEVIATION, str(self.avg_deviation)],
            [GOALS,
             'kafka.autobalancer.goals.NetworkInUsageDistributionGoal,'
             'kafka.autobalancer.goals.NetworkOutUsageDistributionGoal'],
            [IN_THRESHOLD, str(threshold)],
            [OUT_THRESHOLD, str(threshold)],
            [REPORT_INTERVAL, str(4000)],
            [DETECT_INTERVAL, str(8000)],
            [METRIC_REPORTERS, 'kafka.autobalancer.metricsreporter.AutoBalancerMetricsReporter'],
            ['s3.wal.path', FILE_WAL if wal == 'file' else S3_WAL],
        ]

        if exclude_broker:
            server_prop_overrides.append([EXCLUDE_BROKER, exclude_broker])

        if exclude_topic:
            server_prop_overrides.append([EXCLUDE_TOPIC, exclude_topic])

        self.controller_num_nodes_override = 0
        if num_nodes == 3:
            self.controller_num_nodes_override = 1  # only use one combined node

        self.kafka = KafkaService(self.context, num_nodes=num_nodes, zk=None,
                                  kafka_heap_opts="-Xmx2048m -Xms2048m",
                                  server_prop_overrides=server_prop_overrides,
                                  topics={
                                      self.topic: {
                                          'partitions': partition,
                                          'replication-factor': 1,
                                          "replica-assignment": replica_assignment,
                                          'configs': {
                                              'min.insync.replicas': 1,
                                          }
                                      },
                                  },
                                  controller_num_nodes_override=self.controller_num_nodes_override
                                  )
        self.start = True

    @cluster(num_nodes=5)
    @matrix(automq_num_nodes=[2], partition=[4], replica_assignment=['1,1,1,2'], wal=['file', 's3'])
    def test_action(self, automq_num_nodes, partition, replica_assignment, wal):
        """
        Test throughput distribution across brokers
        :param automq_num_nodes: Number of automq
        :param partition: Number of partitions
        :param replica_assignment: Replica assignment for partitions
        """
        success, msg = True, ''
        self.create_kafka(num_nodes=automq_num_nodes, partition=partition, replica_assignment=replica_assignment, wal=wal)
        self.kafka.start()

        run_simple_load(test_context=self.context, kafka=self.kafka, logger=self.logger, topic=self.topic,
                        num_records=20000, throughput=1300)

        topic_after = self.kafka.parse_describe_topic(self.kafka.describe_topic(TOPIC))
        success_, msg_ = check_partition_replicas(topic_after)
        success = success and success_
        msg = append_info(msg, success_, msg_)

        assert success, msg

    @cluster(num_nodes=4)
    @matrix(automq_num_nodes=[2], exclude_broker=['2'], partition=[4], replica_assignment=['1,1,1,2'], wal=['file', 's3'])
    def test_broker_white_list(self, automq_num_nodes, exclude_broker, partition, replica_assignment, wal):
        """
        Test broker exclusion functionality
        :param automq_num_nodes: Number of automq
        :param exclude_broker: Brokers to exclude from AutoBalancer
        :param partition: Number of partitions
        :param replica_assignment: Replica assignment for partitions
        """
        success, msg = True, ''
        self.create_kafka(num_nodes=automq_num_nodes, exclude_broker=exclude_broker, partition=partition,
                          replica_assignment=replica_assignment, wal=wal)
        self.kafka.start()
        before = self.kafka.parse_describe_topic(self.kafka.describe_topic(TOPIC))
        run_simple_load(test_context=self.context, kafka=self.kafka, logger=self.logger, topic=self.topic,
                        num_records=20000, throughput=1300)
        after = self.kafka.parse_describe_topic(self.kafka.describe_topic(TOPIC))

        success_, msg_ = check_partition_eq(topic_info1=before,
         topic_info2=after)
        success = success and success_
        msg = append_info(msg, success_, msg_)

        assert success, msg

    @cluster(num_nodes=6)
    @matrix(automq_num_nodes=[2], wal=['file', 's3'])
    def test_topic_white_list(self, automq_num_nodes, wal):
        """
        Test topic exclusion functionality
        :param automq_num_nodes: Number of automq
        """
        success, msg = True, ''
        topic1 = 'test_topic01'
        topic_cfg1 = {
            "topic": topic1,
            "partitions": 4,
            "replication-factor": 1,
            "configs": {"min.insync.replicas": 1},
            "replica-assignment": '1,1,1,2',
        }
        topic2 = 'test_topic02'
        topic_cfg2 = {
            "topic": topic2,
            "partitions": 4,
            "replication-factor": 1,
            "configs": {"min.insync.replicas": 1},
            "replica-assignment": '1,1,1,2',
        }
        self.create_kafka(num_nodes=automq_num_nodes, exclude_topic=topic1, partition=1, replica_assignment='1', wal=wal)
        self.kafka.start()
        self.kafka.create_topic(topic_cfg1)
        self.kafka.create_topic(topic_cfg2)

        topic1_before = self.kafka.parse_describe_topic(self.kafka.describe_topic(topic1))
        run_simple_load(test_context=self.context, kafka=self.kafka, logger=self.logger, topic=topic1,
                        num_records=15000, throughput=1300)
        run_simple_load(test_context=self.context, kafka=self.kafka, logger=self.logger, topic=topic2,
                        num_records=15000, throughput=1300)

        topic1_after = self.kafka.parse_describe_topic(self.kafka.describe_topic(topic1))
        topic2_after = self.kafka.parse_describe_topic(self.kafka.describe_topic(topic2))

        success_, msg_ = check_partition_eq(topic1_before, topic1_after)
        success = success and success_
        msg = append_info(msg, success_, msg_)

        success_, msg_ = check_partition_replicas(topic2_after)
        success = success and success_
        msg = append_info(msg, success_, msg_)

        assert success, msg
