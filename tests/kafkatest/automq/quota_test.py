"""
Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
"""

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.mark import matrix

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.version import DEV_BRANCH
from kafkatest.automq.automq_e2e_util import FILE_WAL, S3_WAL, publish_broker_configuration


class QuotaTest(Test):
    """
    These tests verify that Broker quota provides expected functionality.
    """
    RECORD_SIZE = 3000

    def __init__(self, test_context):
        super(QuotaTest, self).__init__(test_context=test_context)

        self.topic = 'test_topic'
        self.producer_client_id = 'producer_client'
        self.consumer_client_id = 'consumer_client'

        self.client_version = DEV_BRANCH

        self.broker_id = '1'

        self.maximum_client_deviation_percentage = 100.0
        self.maximum_broker_deviation_percentage = 5

        self.success = True
        self.msg = ''

    def create_kafka(self, test_context, broker_quota_in, broker_quota_out, broker_wal):
        log_size = 256 * 1024 * 1024
        block_size = 256 * 1024 * 1024
        server_prop_overrides = [
            ['broker.quota.enabled', 'true'],
            ['broker.quota.produce.bytes', str(broker_quota_in)],
            ['broker.quota.fetch.bytes', str(broker_quota_out)],
            ['s3.wal.cache.size', str(log_size)],
            ['s3.wal.capacity', str(log_size)],
            ['s3.wal.upload.threshold', str(log_size // 4)],
            ['s3.block.cache.size', str(block_size)],
            ['s3.wal.path', FILE_WAL if broker_wal == 'file' else S3_WAL],
        ]
        self.kafka = KafkaService(test_context, num_nodes=1, zk=None,
                                  kafka_heap_opts="-Xmx2048m -Xms2048m",
                                  server_prop_overrides=server_prop_overrides,
                                  topics={self.topic: {'partitions': 6, 'replication-factor': 1,
                                                       'configs': {'min.insync.replicas': 1}}},
                                  jmx_object_names=['kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec',
                                                    'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec'],
                                  jmx_attributes=['OneMinuteRate'])

    def update_quota_config(self, producer_byte_rate, consumer_byte_rate):
        publish_broker_configuration(self.kafka, producer_byte_rate, consumer_byte_rate, self.broker_id)

    def start_perf_producer(self, num_records, throughput=1000):
        batch_size = 16 * 1024
        buffer_memory = 64 * 1024 * 1024
        self.producer = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=self.topic, num_records=num_records, record_size=QuotaTest.RECORD_SIZE, throughput=throughput,
            client_id=self.producer_client_id, version=self.client_version,
            settings={
                'acks': 1,
                'compression.type': "none",
                'batch.size': batch_size,
                'buffer.memory': buffer_memory
            })
        self.producer.run()

        produced_num = sum([value['records'] for value in self.producer.results])
        assert int(produced_num) == num_records, f"Send count does not match the expected records count: expected {num_records}, but got {produced_num}"

    def start_console_consumer(self):
        consumer_client_id = self.consumer_client_id
        self.consumer = ConsoleConsumer(self.test_context, 1, self.kafka, self.topic,
                                        consumer_timeout_ms=60000, client_id=consumer_client_id,
                                        jmx_object_names=[
                                            'kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s' % consumer_client_id],
                                        jmx_attributes=['bytes-consumed-rate'], version=self.client_version)
        self.consumer.run()
        for idx, messages in self.consumer.messages_consumed.items():
            assert len(messages) > 0, "consumer %d didn't consume any message before timeout" % idx

    @cluster(num_nodes=5)
    @matrix(broker_in=[2500000], broker_out=[2000000], wal=['file', 's3'])
    def test_quota(self, broker_in, broker_out, wal):
        self.create_kafka(self.test_context, broker_in, broker_out, wal)
        self.kafka.start()
        records = 50000
        self.logger.info(f'update to {broker_in},{broker_out}')
        self.do_quota_validate(records, broker_in, broker_out)
        # quota reduction
        broker_in = broker_in // 2
        broker_out = broker_out // 2
        self.update_quota_config(broker_in, broker_out)
        self.logger.info(f'update to {broker_in},{broker_out}')
        self.do_quota_validate(records, broker_in, broker_out)
        assert self.success, self.msg

    def do_quota_validate(self, num_records, broker_in, broker_out):
        # Produce all messages
        self.start_perf_producer(num_records=num_records, throughput=-1)
        # Consume all messages
        self.start_console_consumer()
        # validate
        self.validate(broker=self.kafka, producer=self.producer, consumer=self.consumer,
                      producer_quota=broker_in,
                      consumer_quota=broker_out)

    def validate(self, broker, producer, consumer, producer_quota, consumer_quota):
        """
        For each client_id we validate that:
        1) number of consumed messages equals number of produced messages
        2) maximum_producer_throughput <= producer_quota * (1 + maximum_client_deviation_percentage/100)
        3) maximum_broker_byte_in_rate <= producer_quota * (1 + maximum_broker_deviation_percentage/100)
        4) maximum_avg_broker_byte_in_rate <= producer_quota * (1 + maximum_broker_deviation_percentage/100)
        5) maximum_consumer_throughput <= consumer_quota * (1 + maximum_client_deviation_percentage/100)
        6) maximum_avg_broker_byte_out_rate <= consumer_quota * (1 + maximum_broker_deviation_percentage/100)
        7) maximum_broker_byte_out_rate <= consumer_quota * (1 + maximum_broker_deviation_percentage/100)

        """
        success = True
        msg = '\n'

        broker.read_jmx_output_all_nodes(start_time_sec=broker.jmx_end_time_sec)

        # validate that number of consumed messages equals number of produced messages
        produced_num = sum([value['records'] for value in producer.results])
        consumed_num = sum([len(value) for value in consumer.messages_consumed.values()])
        self.logger.info('producer produced %d messages' % produced_num)
        self.logger.info('consumer consumed %d messages' % consumed_num)
        if produced_num != consumed_num:
            success = False
            msg += "number of produced messages %d doesn't equal number of consumed messages %d\n" % (
                produced_num, consumed_num)

        # validate that maximum_producer_throughput <= producer_quota * (1 + maximum_client_deviation_percentage/100)
        producer_maximum_bps = max(
            metric.value for k, metrics in
            producer.metrics(group='producer-metrics', name='outgoing-byte-rate', client_id=producer.client_id) for
            metric in metrics
        )
        producer_quota_bps = producer_quota
        self.logger.info('producer has maximum throughput %.2f bps with producer quota %.2f bps' % (
            producer_maximum_bps, producer_quota_bps))
        if producer_maximum_bps > producer_quota_bps * (self.maximum_client_deviation_percentage / 100 + 1):
            success = False
            msg += 'maximum producer throughput %.2f bps exceeded producer quota %.2f bps by more than %.1f%%\n' % \
                   (producer_maximum_bps, producer_quota_bps, self.maximum_client_deviation_percentage)

        # validate that maximum_broker_byte_in_rate <= producer_quota * (1 + maximum_broker_deviation_percentage/100)
        broker_byte_in_attribute_name = 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec:OneMinuteRate'
        broker_maximum_byte_in_bps = broker.maximum_jmx_value[broker_byte_in_attribute_name]
        self.logger.info('broker has maximum byte-in rate %.2f bps with producer quota %.2f bps' %
                         (broker_maximum_byte_in_bps, producer_quota_bps))
        if broker_maximum_byte_in_bps > producer_quota_bps * (self.maximum_broker_deviation_percentage / 100 + 1):
            success = False
            msg += 'maximum broker byte-in rate %.2f bps exceeded producer quota %.2f bps by more than %.1f%%\n' % \
                   (broker_maximum_byte_in_bps, producer_quota_bps, self.maximum_broker_deviation_percentage)

        # validate that broker_maximum_avg_byte_in_bps <= producer_quota_bps * (1 + maximum_broker_deviation_percentage/100)
        broker_maximum_avg_byte_in_bps = broker.jmx_window_max_avg_values[broker_byte_in_attribute_name]
        self.logger.info('broker has windows maximum byte-in rate %.2f bps with producer quota %.2f bps' %
                         (broker_maximum_avg_byte_in_bps, producer_quota_bps))
        if broker_maximum_avg_byte_in_bps > producer_quota_bps * (self.maximum_broker_deviation_percentage / 100 + 1):
            success = False
            msg += 'windows maximum average broker byte-in rate %.2f bps exceeded producer quota %.2f bps by more than %.1f%%\n' % \
                   (broker_maximum_avg_byte_in_bps, producer_quota_bps, self.maximum_broker_deviation_percentage)

        # validate that maximum_consumer_throughput <= consumer_quota * (1 + maximum_client_deviation_percentage/100)
        consumer_attribute_name = 'kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s:bytes-consumed-rate' % consumer.client_id
        consumer_maximum_bps = consumer.maximum_jmx_value[consumer_attribute_name]
        consumer_quota_bps = consumer_quota
        self.logger.info('consumer has maximum throughput %.2f bps with consumer quota %.2f bps' % (
            consumer_maximum_bps, consumer_quota_bps))
        if consumer_maximum_bps > consumer_quota_bps * (self.maximum_client_deviation_percentage / 100 + 1):
            success = False
            msg += 'maximum consumer throughput %.2f bps exceeded consumer quota %.2f bps by more than %.1f%%\n' % \
                   (consumer_maximum_bps, consumer_quota_bps, self.maximum_client_deviation_percentage)

        # validate that maximum_broker_byte_out_rate <= consumer_quota * (1 + maximum_broker_deviation_percentage/100)
        broker_byte_out_attribute_name = 'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec:OneMinuteRate'
        broker_maximum_byte_out_bps = broker.maximum_jmx_value[broker_byte_out_attribute_name]
        self.logger.info('broker has maximum byte-out rate %.2f bps with consumer quota %.2f bps' %
                         (broker_maximum_byte_out_bps, consumer_quota_bps))
        if broker_maximum_byte_out_bps > consumer_quota_bps * (self.maximum_broker_deviation_percentage / 100 + 1):
            success = False
            msg += 'maximum broker byte-out rate %.2f bps exceeded consumer quota %.2f bps by more than %.1f%%\n' % \
                   (broker_maximum_byte_out_bps, consumer_quota_bps, self.maximum_broker_deviation_percentage)

        # validate that broker_maximum_avg_byte_out_bps <= consumer_quota * (1 + maximum_broker_deviation_percentage/100)
        broker_maximum_avg_byte_out_bps = broker.jmx_window_max_avg_values[broker_byte_out_attribute_name]
        self.logger.info('broker has windows maximum average byte-out rate %.2f bps with consumer quota %.2f bps' %
                         (broker_maximum_avg_byte_out_bps, consumer_quota_bps))
        if broker_maximum_avg_byte_out_bps > consumer_quota_bps * (self.maximum_broker_deviation_percentage / 100 + 1):
            success = False
            msg += 'windows maximum average broker byte-out rate %.2f bps exceeded consumer quota %.2f bps by more than %.1f%%\n' % \
                   (broker_maximum_avg_byte_out_bps, consumer_quota_bps, self.maximum_broker_deviation_percentage)

        if self.success and not success:
            self.success = False
        self.msg += msg
