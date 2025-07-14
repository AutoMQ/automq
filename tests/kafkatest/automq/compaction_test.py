"""
Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
"""

import time

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.automq.automq_e2e_util import (TOPIC, FILE_WAL, S3_WAL, formatted_time,
                                              ensure_stream_set_object_compaction,
                                              ensure_stream_object_compaction,
                                              STREAM_OBJECT_COMPACTION_TYPE_MAJOR_V1,
                                              STREAM_OBJECT_COMPACTION_TYPE_MINOR_V1,
                                              run_validation_producer, append_info, run_console_consumer
, correctness_verification, parse_delta_wal_entry)

from kafkatest.services.kafka import KafkaService
from ducktape.mark import matrix

from kafkatest.utils import is_int_with_prefix


class CompactionTest(Test):
    """
    Test Compaction
    """
    STREAM_OBJECT_COMPACTION_CHECK_INTERVAL_MIN = 1
    INT32_MAX = int(2**31 - 1)

    def __init__(self, test_context):
        super(CompactionTest, self).__init__(test_context)
        self.context = test_context
        self.topic = TOPIC
        self.stream_set_compaction_minute = 1
        self.stream_compaction_minute = 1
        self.s3_wal_upload_threshold = 50 * 1024
        self.automq_stream_object_compaction_jitter_max_delay_minute = 1

    def create_kafka(self, num_nodes=1, partition=1, broker_wal='s3', env=None):
        """
        Create and configure Kafka service.

        Args:
            :param env: additional environmental variables
            :param num_nodes: Number of Kafka nodes.
            :param partition: Number of partitions.
            :param broker_wal: wal
        """
        log_size = 256 * 1024 * 1024
        block_size = 256 * 1024 * 1024
        server_prop_overrides = [
            ['s3.wal.cache.size', str(log_size)],
            ['s3.wal.capacity', str(log_size)],
            ['s3.wal.upload.threshold', str(log_size // 4)],
            ['s3.block.cache.size', str(block_size)],
            ['s3.stream.set.object.compaction.interval.minutes', str(self.stream_set_compaction_minute)],
            ['autobalancer.controller.enable', 'false'],
            ['s3.wal.upload.threshold', str(int(self.s3_wal_upload_threshold))],
            ['s3.wal.path', FILE_WAL if broker_wal == 'file' else S3_WAL],
            ['s3.stream.set.object.compaction.stream.split.size', str(self.automq_stream_object_compaction_jitter_max_delay_minute)], # set s3.stream.set.object.compact.stream.split.size == 1 to ensure that there is no remaining stream set object
            ['s3.stream.object.split.size', '1024'] # get enough stream objects when no stream set compaction occurs

        ]

        extra_env = [
            f'AUTOMQ_STREAM_OBJECT_COMPACTION_JITTER_MAX_DELAY=1',  # reduce the delay of the first triggering task
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

    @cluster(num_nodes=4)
    @matrix(stream_set_object_compaction=[True, False],
            stream_object_compaction_type=[STREAM_OBJECT_COMPACTION_TYPE_MINOR_V1, STREAM_OBJECT_COMPACTION_TYPE_MAJOR_V1], wal=['s3'])
    @matrix(stream_set_object_compaction=[True], stream_object_compaction_type=['None'], wal=['s3'])
    def test_case(self, stream_set_object_compaction, stream_object_compaction_type, wal):
        '''

        :param stream_set_object_compaction:
        :param stream_object_compaction_type:
        :param wal:
        :return:
        '''
        self.run0(stream_set_object_compaction, stream_object_compaction_type, wal)

    def run0(self, stream_set_object_compaction=False,
             stream_object_compaction_type=STREAM_OBJECT_COMPACTION_TYPE_MINOR_V1, wal=S3_WAL):
        """
        Run the test with specified compaction type.

        Args:
            :param stream_set_object_compaction:
            :param env: env
            :param stream_object_compaction_type: type of the stream object compaction.
            :param wal: wal
        """
        if stream_set_object_compaction is False and stream_object_compaction_type is None:
            return

        if stream_object_compaction_type == STREAM_OBJECT_COMPACTION_TYPE_MAJOR_V1:
            env = [f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_COMPACTION_SIZE_THRESHOLD=0',
                   f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_INTERVAL={str(self.INT32_MAX)}',
                   f'AUTOMQ_STREAM_COMPACTION_MAJOR_V1_INTERVAL={str(30 * 1000)}']
        elif stream_object_compaction_type == STREAM_OBJECT_COMPACTION_TYPE_MINOR_V1:
            env = [f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_COMPACTION_SIZE_THRESHOLD=419430400',
                   f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_INTERVAL={str(30 * 1000)}',
                   f'AUTOMQ_STREAM_COMPACTION_MAJOR_V1_INTERVAL={str(self.INT32_MAX)}']
        else:
            env = [f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_INTERVAL={str(self.INT32_MAX)}',
                   f'AUTOMQ_STREAM_COMPACTION_MAJOR_V1_INTERVAL={str(self.INT32_MAX)}']

        if not stream_set_object_compaction:
            self.stream_set_compaction_minute = self.INT32_MAX

        self.compaction_delay_sec = (self.automq_stream_object_compaction_jitter_max_delay_minute +
                                     self.STREAM_OBJECT_COMPACTION_CHECK_INTERVAL_MIN) * 60
        self.create_kafka(broker_wal=wal, env=env)
        self.kafka.start()

        success = True
        msg = ''

        # Stream object source: 1. Split the stream set compaction into 2. Split when uploading to S3
        producer1 = run_validation_producer(self.kafka, self.test_context, self.logger, self.topic, 120000, 6000, message_validator=is_int_with_prefix)
        if stream_set_object_compaction:
            time.sleep(self.compaction_delay_sec)
        producer2 = run_validation_producer(self.kafka, self.test_context, self.logger, self.topic, 120000, 6000, message_validator=is_int_with_prefix)
        start_time = formatted_time().split(' ')[-1]
        self.logger.info(f"compaction_delay_sec is {self.compaction_delay_sec} sec.")
        time.sleep(self.compaction_delay_sec)
        end_time = formatted_time().split(' ')[-1]

        consumer = run_console_consumer(self.test_context, self.kafka, topic=self.topic, message_validator=is_int_with_prefix)

        success_, msg_ = correctness_verification(self.logger, [producer1, producer2], consumer)
        success = success and success_
        msg = append_info(msg, success_, msg_)

        stream_set_object, stream_object, delta_wal_entry_count = parse_delta_wal_entry(self.kafka, self.logger)
        assert delta_wal_entry_count > 0, (
            f"delta_wal_entry_count ({delta_wal_entry_count}) must be greater than 0."
        )

        self.logger.info(f'The compaction(stream set obeject compaction and stream object compaction) time interval is {start_time} ——> {end_time}')
        if stream_set_object_compaction:
            ensure_stream_set_object_compaction(self.kafka, start_time, end_time)
        if stream_object_compaction_type != 'None':
            ensure_stream_object_compaction(self.kafka, stream_object_compaction_type, start_time, end_time)

        assert success, msg
