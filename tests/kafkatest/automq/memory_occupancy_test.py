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
import time
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.version import DEV_BRANCH, KafkaVersion
from kafkatest.services.performance import ProducerPerformanceService, ConsumerPerformanceService
from kafkatest.automq.automq_e2e_util import formatted_time, parse_log_entry, parse_producer_performance_stdout


class TestJVMMemoryOccupancy(Test):
    """
    Test the memory usage
    """

    def __init__(self, test_context):
        super(TestJVMMemoryOccupancy, self).__init__(test_context)
        self.context = test_context
        self.last_consumed_offsets = {}
        self.topic = "topic"
        self.consume_group = 'test_group'
        self.records_consumed = []

    def create_kafka(self, num_nodes=1, partition=None, log_size=None, block_size=None, **kwargs):
        """
        Create and configure Kafka service.

        :param num_nodes: Number of Kafka nodes.
        :param partition: Number of partitions for the topic.
        :param log_size: Log size for Kafka configuration.
        :param block_size: Block size for Kafka configuration.
        """
        topics = {
            self.topic: {"partitions": partition, "replication-factor": 1}
        }

        self.server_prop_overrides = [
            ['s3.wal.cache.size', str(log_size)],
            ['s3.wal.capacity', str(log_size)],
            ['s3.wal.upload.threshold', str(log_size // 4)],
            ['s3.block.cache.size', str(block_size)]
        ]

        self.kafka = KafkaService(
            self.test_context,
            num_nodes=num_nodes,
            topics=topics,
            server_prop_overrides=self.server_prop_overrides,
            zk=None,
            kafka_heap_opts = "-Xmx2048m -Xms2048m",
            extra_env=[
                'AUTOMQ_MEMORY_USAGE_DETECT="true"',
                'AUTOMQ_MEMORY_USAGE_DETECT_TIME_INTERVAL=2000'
            ],
            **kwargs
        )

    def check_properties(self, msg, expect):
        """
        Check the Kafka configuration properties.

        :param msg: Property name to check.
        :param expect: Expected value of the property.
        """
        for line in self.kafka.nodes[0].account.ssh_capture('grep ' + msg + ' ' + self.kafka.CONFIG_FILE):
            result = line.strip('\n').split('=', 1)
            if msg == result[0]:
                self.logger.info(result)
                assert expect == result[1], f'Error set env for Kafka: {expect} != {result[1]}'

    def check_the_production_quantity(self, records):
        """
        Check the production quantity
        """
        last_line = ''
        for line in self.producer.nodes[0].account.ssh_capture('cat ' + self.producer.STDOUT_CAPTURE):
            last_line = line
            self.logger.info(f'producer.log:{line}')
        send_num = parse_producer_performance_stdout(last_line)['records_sent']
        assert int(send_num) == records, f"Send count does not match the expected records count: expected {records}, but got {send_num}"

    def check_the_consumption_quantity(self, records):
        """
        Check the consumption quantity
        """
        last_line = ''
        for line in self.consumer.nodes[0].account.ssh_capture('cat ' + self.consumer.STDOUT_CAPTURE):
            last_line = line
            self.logger.info(f'consumer.log:{line}')
        receive_num = last_line.split(',')[4].strip()
        assert int(receive_num) == records, f"Receive count does not match the expected records count: expected {records}, but got {receive_num}"

    @cluster(num_nodes=3)
    @matrix(partition=[128, 512], log_size=[256 * 1024 * 1024], block_size=[128 * 1024 * 1024, 256 * 1024 * 1024])
    def test(self, partition, log_size, block_size):
        """
        At any time, 1/writable record in Metric<=log cache size+100MB
        At any time, 11/block_cache in Metric<=block cache size

        :param partition: Number of partitions for the topic.
        :param log_size: Log size for Kafka configuration.
        :param block_size: Block size for Kafka configuration.
        """
        # Start Kafka
        self.create_kafka(partition=partition, log_size=log_size, block_size=block_size)
        self.kafka.start()

        # Check Kafka configuration
        self.check_properties('s3.wal.cache.size', str(log_size))
        self.check_properties('s3.block.cache.size', str(block_size))

        time1 = time.time()
        self.logger.info(formatted_time('Producer start time: '))

        # Start producer
        record_size = 128
        target_data_size = 512 * 1024 * 1024
        batch_size = 16 * 1024
        buffer_memory = 64 * 1024 * 1024
        throughput = 80000
        records = int(target_data_size // record_size)
        self.logger.info(f'Target Data Size: {target_data_size}')

        self.producer = ProducerPerformanceService(
            self.test_context, num_nodes=1, kafka=self.kafka, topic=self.topic,
            num_records=records, record_size=record_size, throughput=throughput, version=KafkaVersion(str(DEV_BRANCH)),
            settings={
                'acks': 1,
                'compression.type': "none",
                'batch.size': batch_size,
                'buffer.memory': buffer_memory
            }
        )
        self.producer.run()

        # Check the quantity sent
        self.check_the_production_quantity(records)

        time2 = time.time()
        self.logger.info(f"Producer execution time: {(time2 - time1):.4f} seconds")
        self.logger.info(formatted_time('Producer end time, Consumer start time: '))

        # Start consumer
        self.consumer = ConsumerPerformanceService(
            self.test_context, num_nodes=1, kafka=self.kafka,
            topic=self.topic, messages=records
        )
        self.consumer.run()

        # Check the quantity receive
        self.check_the_consumption_quantity(records)

        time3 = time.time()
        self.logger.info(formatted_time('Consumer end time: '))
        self.logger.info(f"Consumer execution time: {(time3 - time2):.4f} seconds")

        # server.log and validate buffer usage
        count = 0
        for line in self.kafka.nodes[0].account.ssh_capture('cat ' + self.kafka.STDOUT_STDERR_CAPTURE):
            if len(line.rstrip()) > 0 and 'INFO Buffer usage' in line:
                log_dict = parse_log_entry(line)
                if '1/write_record' in log_dict['buffer_usage']:
                    write_record = log_dict['buffer_usage']['1/write_record']
                    assert int(write_record) <= 100 * 1024 * 1024 + log_size, \
                        f"Error: '1/write_record' buffer usage exceeded limit. Actual: {write_record}, Limit: {100 * 1024 * 1024 + log_size}"
                if '11/block_cache' in log_dict['buffer_usage']:
                    block_cache = log_dict['buffer_usage']['11/block_cache']
                    assert int(block_cache) <= block_size, \
                        f"Error: '11/block_cache' buffer usage exceeded limit. Actual: {block_cache}, Limit: {block_size}"
                count += 1
                self.logger.info(f'server.log:{line}')
        assert count > 0, "Error: No 'INFO Buffer usage' entries found in the log."
        self.logger.info(f'The number of INFO Buffer usage is {count}')
