"""
Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
"""

import time
import re

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.automq.automq_e2e_util import (TOPIC, FILE_WAL, S3_WAL, run_perf_producer, formatted_time,
                                              capture_and_filter_logs, ensure_stream_object_compaction,
                                              ensure_stream_set_object_compaction,
                                              STREAM_OBJECT_COMPACTION_TYPE_MAJOR_V1,
                                              STREAM_OBJECT_COMPACTION_TYPE_MINOR_V1, parse_delta_wal_entry
                                              )
from kafkatest.services.kafka import KafkaService
from ducktape.mark import matrix


class LeakTest(Test):
    """
    Test class for detecting data leaks in Kafka service.
    """
    STREAM_OBJECT_COMPACTION_CHECK_INTERVAL_MIN = 1
    OBJECT_LIFECYCLE_CHECK_INTERVAL_MIN = 1

    def __init__(self, test_context):
        super(LeakTest, self).__init__(test_context)
        self.context = test_context
        self.topic = TOPIC
        self.s3_delete_retention_min = 1
        self.stream_set_compaction_min = 1

        self.minor_v1_compaction_interval = 1
        self.major_v1_compaction_interval = 0.5
        self.s3_wal_upload_threshold = 16 * 1024 * 1024
        self.automq_stream_object_compaction_jitter_max_delay = 1

    def create_kafka(self, num_nodes=1, partition=1, broker_wal='s3', env=None):
        """
        Create and configure Kafka service.

        Args:
            :param num_nodes: Number of Kafka nodes.
            :param partition: Number of partitions.
            :param broker_wal: wal
        """
        if env is None:
            env = []
        log_size = 256 * 1024 * 1024
        block_size = 256 * 1024 * 1024
        server_prop_overrides = [
            ['s3.wal.cache.size', str(log_size)],
            ['s3.wal.capacity', str(log_size)],
            ['s3.wal.upload.threshold', str(log_size // 4)],
            ['s3.block.cache.size', str(block_size)],
            ['s3.object.delete.retention.minutes', str(self.s3_delete_retention_min)],
            ['s3.stream.set.object.compaction.interval.minutes', str(self.stream_set_compaction_min)],
            ['autobalancer.controller.enable', 'false'],
            ['s3.wal.upload.threshold', str(int(self.s3_wal_upload_threshold))],
            ['s3.wal.path', FILE_WAL if broker_wal == 'file' else S3_WAL],
            ['s3.stream.set.object.compaction.stream.split.size', '1'], # set s3.stream.set.object.compact.stream.split.size == 1 to ensure that there is no remaining stream set object
        ]
        extra_env = [
            f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_INTERVAL={str(int(self.minor_v1_compaction_interval * 60 * 1000))}',
            f'AUTOMQ_STREAM_COMPACTION_MAJOR_V1_INTERVAL={str(int(self.major_v1_compaction_interval * 60 * 1000))}',
            f'AUTOMQ_STREAM_OBJECT_COMPACTION_JITTER_MAX_DELAY=1',# reduce the delay of the first triggering task
        ]
        if env:
            extra_env.extend(env)
        self.kafka = KafkaService(self.context, num_nodes=num_nodes, zk=None,
                                  kafka_heap_opts="-Xmx2048m -Xms2048m",
                                  server_prop_overrides=server_prop_overrides,
                                  extra_env=extra_env,
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

    def run0(self, stream_object_compaction_type, wal, env=None):
        """
        Run the test with specified compaction type.

        Args:
            :param env: env
            :param stream_object_compaction_type: type of the stream object compaction.
            :param wal: wal
        """
        self.compaction_delay_sec = (self.automq_stream_object_compaction_jitter_max_delay + self.STREAM_OBJECT_COMPACTION_CHECK_INTERVAL_MIN) * 60
        self.s3_delete_delay_sec = (self.s3_delete_retention_min + self.OBJECT_LIFECYCLE_CHECK_INTERVAL_MIN + 0.5) * 60
        self.create_kafka(broker_wal=wal, env=env)
        self.kafka.start()
        self.logger.info(f"compaction_delay_sec is {self.compaction_delay_sec} sec.")
        self.logger.info(f"s3_delete_delay_sec is {self.s3_delete_delay_sec} sec.")
        run_perf_producer(test_context=self.context, kafka=self.kafka, num_records=20000, topic=self.topic,
                          throughput=5000)
        self.kafka.restart_cluster()

        objects = self.kafka.get_bucket_objects()
        time_of_restart_cluster = formatted_time()
        start_time = time_of_restart_cluster.split(' ')[-1]
        self.logger.info(f'[RESTART_CLUSTER][{time_of_restart_cluster}], before {objects}')
        time.sleep(self.compaction_delay_sec)
        end_time = formatted_time().split(' ')[-1]
        # ensure that the stream set objects comparison is executed
        ensure_stream_set_object_compaction(self.kafka, start_time, end_time)
        # ensure that the stream objects comparison is executed
        ensure_stream_object_compaction(self.kafka, stream_object_compaction_type, start_time, end_time)

        self.kafka.delete_topic(self.topic)
        time_of_delete_topic = formatted_time()
        self.logger.info(f'[DELETE_TOPIC][{time_of_delete_topic}], topic is {self.topic}')
        time.sleep(self.s3_delete_delay_sec)
        objects, after = self.kafka.get_bucket_objects()
        time_of_after_wait = formatted_time()
        self.logger.info(f'[AFTER_WAIT][{time_of_after_wait}], after {objects}')

        stream_set_object, stream_object, delta_wal_entry_count = parse_delta_wal_entry(self.kafka, self.logger)
        assert delta_wal_entry_count > 0, (
            f"delta_wal_entry_count ({delta_wal_entry_count}) must be greater than 0."
        )

        stream_set_object_count = 0
        stream_object_count = 0
        for obj in objects:
            object_id = obj['path'].rsplit('/', 1)[-1]
            if object_id.startswith('node-') or object_id.startswith('wal'):
                continue
            if object_id in stream_set_object:
                stream_set_object_count += 1
            if object_id in stream_object:
                stream_object_count += 1

        assert stream_set_object_count == 0 and stream_object_count == 0, (
            f"Assertion failed: stream_set_object_count ({stream_set_object_count}) must be 0. "
            f"and stream_object_count ({stream_object_count}) must be 0."
        )

    @cluster(num_nodes=2)
    @matrix(wal=['s3'])
    def test_s3_leak_major_v1(self, wal):
        """
        Test S3 leak with major V1 compaction.
        """
        self.minor_v1_compaction_interval = 1000  # far greater than major_V1_compaction_interval
        self.major_v1_compaction_interval = 0.5
        self.run0(stream_object_compaction_type=STREAM_OBJECT_COMPACTION_TYPE_MAJOR_V1, wal=wal, env=[f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_COMPACTION_SIZE_THRESHOLD=0'])

    @cluster(num_nodes=2)
    @matrix(wal=['s3'])
    def test_s3_leak_minor_v1(self, wal):
        """
        Test S3 leak with minor V1 compaction.
        """
        self.minor_v1_compaction_interval = 0.5
        self.major_v1_compaction_interval = 1000  # far greater than minor_v1_compaction_interval
        self.run0(stream_object_compaction_type=STREAM_OBJECT_COMPACTION_TYPE_MINOR_V1, wal=wal, env=[
            f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_COMPACTION_SIZE_THRESHOLD=419430400' # ensure that multiple larger stream objects can be merged
        ])
