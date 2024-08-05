"""
Copyright 2024, AutoMQ HK Limited.

The use of this file is governed by the Business Source License,
as detailed in the file "/LICENSE.S3Stream" included in this repository.

As of the Change Date specified in that file, in accordance with
the Business Source License, use of this software will be governed
by the Apache License, Version 2.0
"""

import time
import re

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.automq.automq_e2e_util import (TOPIC, FILE_WAL, S3_WAL, run_perf_producer, formatted_time, capture_and_filter_logs)
from kafkatest.services.kafka import KafkaService
from ducktape.mark import matrix

def parse_upload_delta_wal_log_entry(log_entry):
    """
    Parses a WAL log entry to extract relevant data.

    Args:
        log_entry (str): WAL log entry string.

    Returns:
        tuple: Two dictionaries containing parsed data.
    """
    request_pattern = re.compile(
        r"CommitStreamSetObjectRequest\{objectId=(\d+), orderId=(\d+), objectSize=(\d+), streamRanges=\[(.*?)\], streamObjects=\[(.*?)\], compactedObjectIds=(.*?), attributes=(\d+)\}"
    )
    stream_object_pattern = re.compile(
        r"StreamObject\{objectId=(\d+), objectSize=(\d+), streamId=(\d+), startOffset=(\d+), endOffset=(\d+), attributes=(\d+)\}"
    )
    match = request_pattern.search(log_entry)
    if match:
        object_id = match.group(1)
        order_id = match.group(2)
        object_size = match.group(3)
        stream_ranges = match.group(4)
        stream_objects_str = match.group(5)
        compacted_object_ids = match.group(6)
        attributes = match.group(7)
        main_object = {
            "orderId": order_id,
            "objectSize": object_size,
            "streamRanges": stream_ranges,
            "compactedObjectIds": compacted_object_ids,
            "attributes": attributes
        }
        map1 = {object_id: main_object}
        map2 = {}
        stream_objects = stream_object_pattern.findall(stream_objects_str)
        for stream_object in stream_objects:
            stream_object_id, stream_object_size, stream_id, start_offset, end_offset, stream_attributes = stream_object
            map2[stream_object_id] = {
                "objectSize": stream_object_size,
                "streamId": stream_id,
                "startOffset": start_offset,
                "endOffset": end_offset,
                "attributes": stream_attributes
            }
        return map1, map2
    else:
        return {}, {}


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

    def create_kafka(self, num_nodes=1, partition=1, broker_wal='file'):
        """
        Create and configure Kafka service.

        Args:
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
            ['s3.object.delete.retention.minutes', str(self.s3_delete_retention_min)],
            ['s3.stream.set.object.compaction.interval.minutes', str(self.stream_set_compaction_min)],
            ['autobalancer.controller.enable', 'false'],
            ['s3.wal.upload.threshold', str(int(self.s3_wal_upload_threshold))],
            ['s3.wal.path', FILE_WAL if broker_wal == 'file' else S3_WAL],
        ]
        self.kafka = KafkaService(self.context, num_nodes=num_nodes, zk=None,
                                  kafka_heap_opts="-Xmx2048m -Xms2048m",
                                  server_prop_overrides=server_prop_overrides,
                                  extra_env=[
                                      f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_INTERVAL={str(int(self.minor_v1_compaction_interval * 60 * 1000))}',
                                      f'AUTOMQ_STREAM_COMPACTION_MAJOR_V1_INTERVAL={str(int(self.major_v1_compaction_interval * 60 * 1000))}',
                                      f'AUTOMQ_STREAM_COMPACTION_MINOR_V1_COMPACTION_SIZE_THRESHOLD=0',
                                      f'AUTOMQ_STREAM_OBJECT_COMPACTION_JITTER_MAX_DELAY=1',
                                  ],
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

    def run0(self, stream_object_compaction_type, wal):
        """
        Run the test with specified compaction type.

        Args:
            :param stream_object_compaction_type: type of the stream object compaction.
            :param wal: wal
        """
        self.compaction_delay_sec = (self.automq_stream_object_compaction_jitter_max_delay + self.STREAM_OBJECT_COMPACTION_CHECK_INTERVAL_MIN) * 60
        self.s3_delete_delay_sec = (self.s3_delete_retention_min + self.OBJECT_LIFECYCLE_CHECK_INTERVAL_MIN + 0.5) * 60
        self.create_kafka(broker_wal=wal)
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
        self.kafka.delete_topic(self.topic)
        time_of_delete_topic = formatted_time()
        end_time = time_of_delete_topic.split(' ')[-1]
        self.logger.info(f'[DELETE_TOPIC][{time_of_delete_topic}], topic is {self.topic}')
        time.sleep(self.s3_delete_delay_sec)
        objects, after = self.kafka.get_bucket_objects()
        time_of_after_wait = formatted_time()
        self.logger.info(f'[AFTER_WAIT][{time_of_after_wait}], after {objects}')

        entry = 'stream set objects to compact after filter'
        pattern = re.compile(r"(\d+) stream set objects to compact after filter")
        stream_set_object_compaction_count = 0
        for line in capture_and_filter_logs(self.kafka, start_time=start_time, end_time=end_time, entry=entry):
            match = pattern.search(line)
            if match:
                stream_set_object_compaction_count += 1 if int(match.group().split()[0]) > 0 else 0
        assert stream_set_object_compaction_count > 0, f'before deleting a topic, it must go through a stream set object comparison.'

        entry = f'Compact stream finished, {stream_object_compaction_type}'
        stream_object_compaction_count = 0
        for _ in capture_and_filter_logs(self.kafka, start_time=start_time, end_time=end_time, entry=entry):
            stream_object_compaction_count += 1
        assert stream_object_compaction_count > 0, f'before deleting a topic, it must go through a stream object comparison(type:{stream_object_compaction_type}).'

        stream_set_object = {}
        stream_object = {}
        entry = 'INFO Upload delta WAL finished'
        delta_wal_entry_count = 0
        for line in self.kafka.nodes[0].account.ssh_capture(f'grep \'{entry}\' {self.kafka.STDOUT_STDERR_CAPTURE}'):
            delta_wal_entry_count += 1
            stream_set_object_, stream_object_ = parse_upload_delta_wal_log_entry(line)
            stream_set_object.update(stream_set_object_)
            stream_object.update(stream_object_)

        assert delta_wal_entry_count > 0, (
            f"delta_wal_entry_count ({delta_wal_entry_count}) must be greater than 0."
        )
        self.logger.info(f'{stream_set_object}')
        self.logger.info(f'{stream_object}')

        stream_set_object_count = 0
        stream_object_count = 0
        for obj in objects:
            object_id = obj['path'].rsplit('/', 1)[-1]
            if object_id.startswith('node-'):
                continue
            if object_id in stream_set_object:
                stream_set_object_count += 1
            if object_id in stream_object:
                stream_object_count += 1

        assert stream_set_object_count <= 1 and stream_object_count == 0, (
            f"Assertion failed: stream_set_object_count ({stream_set_object_count}) must be less than or equal to 1 "
            f"and stream_object_count ({stream_object_count}) must be 0."
        )

    @cluster(num_nodes=2)
    @matrix(wal=['file', 's3'])
    def test_s3_leak_major_v1(self, wal):
        """
        Test S3 leak with major V1 compaction.
        """
        self.minor_v1_compaction_interval = 1000  # far greater than major_V1_compaction_interval
        self.major_v1_compaction_interval = 0.5
        self.run0(stream_object_compaction_type='MAJOR_V1', wal=wal)

    @cluster(num_nodes=2)
    @matrix(wal=['file', 's3'])
    def test_s3_leak_minor_v1(self, wal):
        """
        Test S3 leak with minor V1 compaction.
        """
        self.minor_v1_compaction_interval = 0.5
        self.major_v1_compaction_interval = 1000  # far greater than minor_v1_compaction_interval
        self.run0(stream_object_compaction_type='MINOR_V1', wal=wal)
