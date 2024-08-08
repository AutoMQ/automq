# Copyright 2024, AutoMQ HK Limited.
#
# The use of this file is governed by the Business Source License,
# as detailed in the file "/LICENSE.S3Stream" included in this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import re
import time

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.version import DEV_BRANCH


def formatted_time(msg=''):
    """
    formatted the current local time with milliseconds appended to the provided message.
    """
    current_time = time.time()
    local_time = time.localtime(current_time)
    formatted_time = time.strftime('%Y-%m-%d %H:%M:%S', local_time)
    milliseconds = int((current_time - int(current_time)) * 1000)
    formatted_time_with_ms = f"{formatted_time},{milliseconds:03d}"
    return msg + formatted_time_with_ms


def parse_log_entry(log_entry):
    """
    Resolve buffer usage in the server. log
    """
    log_pattern = r'\[(.*?)\] (INFO|ERROR|DEBUG|WARN) (.*)'
    match = re.match(log_pattern, log_entry)
    if not match:
        return None

    timestamp_str, log_level, message = match.groups()
    timestamp = timestamp_str

    buffer_usage_pattern = r'Buffer usage: ByteBufAllocMetric\{(.*)\} \(.*\)'
    buffer_match = re.search(buffer_usage_pattern, message)
    if not buffer_match:
        return None

    buffer_details = buffer_match.group(1).replace(';', ',')
    buffer_parts = buffer_details.split(', ')

    buffer_dict = {}
    for part in buffer_parts:
        if '=' in part:
            key, value = part.split('=', 1)
            buffer_dict[key.strip()] = value.strip()

    log_dict = {
        'timestamp': timestamp,
        'log_level': log_level,
        'buffer_usage': buffer_dict
    }

    return log_dict


def parse_producer_performance_stdout(input_string):
    """
    Resolve the last line of producer_performance.stdout
    """
    pattern = re.compile(
        r"(?P<records_sent>\d+) records sent, "
        r"(?P<records_per_sec>[\d.]+) records/sec \([\d.]+ MB/sec\), "
        r"(?P<avg_latency>[\d.]+) ms avg latency, "
        r"(?P<max_latency>[\d.]+) ms max latency, "
        r"(?P<latency_50th>\d+) ms 50th, "
        r"(?P<latency_95th>\d+) ms 95th, "
        r"(?P<latency_99th>\d+) ms 99th, "
        r"(?P<latency_999th>\d+) ms 99.9th."
    )
    match = pattern.match(input_string)

    if match:
        data = match.groupdict()
        extracted_data = {
            'records_sent': int(data['records_sent']),
            'records_per_sec': float(data['records_per_sec']),
            'avg_latency': float(data['avg_latency']),
            'max_latency': float(data['max_latency']),
            'latency_50th': int(data['latency_50th']),
            'latency_95th': int(data['latency_95th']),
            'latency_99th': int(data['latency_99th']),
            'latency_999th': int(data['latency_999th'])
        }
        return extracted_data
    else:
        raise ValueError("Input string does not match the expected format.")


def publish_broker_configuration(kafka, producer_byte_rate, consumer_byte_rate, broker_id):
    """
    Publish broker configuration to alter the producer and consumer byte rate.
    """
    force_use_zk_connection = False
    node = kafka.nodes[0]
    cmd = "%s --alter --add-config broker.quota.produce.bytes=%d,broker.quota.fetch.bytes=%d" % \
          (kafka.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection), producer_byte_rate,
           consumer_byte_rate)
    cmd += " --entity-type " + 'brokers'
    cmd += " --entity-name " + broker_id
    node.account.ssh(cmd)


RECORD_SIZE = 3000
RECORD_NUM = 50000
TOPIC = 'test_topic'
CONSUMER_GROUP = "test_consume_group"
DEFAULT_CONSUMER_CLIENT_ID = 'test_producer_client_id'
DEFAULT_PRODUCER_CLIENT_ID = 'test_producer_client_id'
BATCH_SIZE = 16 * 1024
BUFFER_MEMORY = 64 * 1024 * 1024
DEFAULT_THROUGHPUT = -1
DEFAULT_CLIENT_VERSION = DEV_BRANCH
JMX_BROKER_IN = 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec'
JMX_BROKER_OUT = 'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec'
JMX_TOPIC_IN = f'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic={TOPIC}'
JMX_TOPIC_OUT = f'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic={TOPIC}'
JMX_ONE_MIN = ':OneMinuteRate'
FILE_WAL = '0@file:///mnt/kafka/kafka-data-logs-1/s3wal'
S3_WAL = '0@s3://ko3?region=us-east-1&endpoint=http://10.5.0.2:4566&pathStyle=false&batchInterval=100&maxBytesInBatch=4194304&maxUnflushedBytes=1073741824&maxInflightUploadCount=50'


def run_perf_producer(test_context, kafka, num_records=RECORD_NUM, throughput=DEFAULT_THROUGHPUT,
                      client_version=DEFAULT_CLIENT_VERSION, topic=TOPIC):
    """
    Initiates and runs a performance test for the producer.

    This function creates and executes a Producer Performance Service to test the performance of sending messages under specified conditions.

    :param test_context: A test context object providing information about the test environment.
    :param kafka: An instance of Kafka for producing messages.
    :param num_records: The number of messages to send, default is RECORD_NUM.
    :param throughput: Specified throughput, default is DEFAULT_THROUGHPUT.
    :param client_version: The version of the client, default is DEFAULT_CLIENT_VERSION.
    :param topic: The target topic for message delivery, default is TOPIC.

    :return: An instance of ProducerPerformanceService used for testing producer performance.
    """
    producer = ProducerPerformanceService(
        test_context, 1, kafka,
        topic=topic, num_records=num_records, record_size=RECORD_SIZE, throughput=throughput,
        client_id=DEFAULT_PRODUCER_CLIENT_ID, version=client_version,
        settings={
            'acks': 1,
            'compression.type': "none",
            'batch.size': BATCH_SIZE,
            'buffer.memory': BUFFER_MEMORY
        })
    producer.run()
    return producer


def run_console_consumer(test_context, kafka, client_version=DEFAULT_CLIENT_VERSION, topic=TOPIC):
    """
    Launches a console consumer to consume messages from a specified topic.

    :param test_context: A test context object that provides configuration information for the test environment.
    :param kafka: A Kafka instance for connecting to and operating on a Kafka cluster.
    :param client_version: The version of the consumer client, defaulting to DEFAULT_CLIENT_VERSION.
    :param topic: The topic from which the consumer will consume messages, defaulting to TOPIC.

    :return: The launched ConsoleConsumer instance.
    """
    consumer = ConsoleConsumer(test_context, 1, topic=topic, kafka=kafka,
                               consumer_timeout_ms=60000, client_id=DEFAULT_CONSUMER_CLIENT_ID,
                               jmx_object_names=[
                                   'kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s' % DEFAULT_CONSUMER_CLIENT_ID],
                               jmx_attributes=['bytes-consumed-rate'], version=client_version)
    consumer.run()
    for idx, messages in consumer.messages_consumed.items():
        assert len(messages) > 0, "consumer %d didn't consume any message before timeout" % idx
    return consumer


def validate_num(producer, consumer, logger):
    """
    Validates the consistency of the number of messages produced and consumed.

    :param producer: The producer object, containing the results of message production.
    :param consumer: The consumer object, containing the consumed messages.
    :param logger: The logging object, used for logging information.

    :return: A tuple containing a boolean value indicating whether the validation passed and a string containing the validation failure message.
    """
    success = True
    msg = ''
    produced_num = sum([value['records'] for value in producer.results])
    consumed_num = sum([len(value) for value in consumer.messages_consumed.values()])
    logger.info('producer produced %d messages' % produced_num)
    logger.info('consumer consumed %d messages' % consumed_num)
    if produced_num != consumed_num:
        success = False
        msg += "number of produced messages %d doesn't equal number of consumed messages %d\n" % (
            produced_num, consumed_num)
    return success, msg


def run_simple_load(test_context, kafka, logger, topic=TOPIC, num_records=RECORD_NUM, throughput=DEFAULT_THROUGHPUT):
    """
    Execute a simple load test.

    This function initializes a performance producer and a console consumer to send and receive a specified number of messages.
    Then, it validates that the number of messages received by the consumer matches the expected count.

    :param test_context: Test context object for storing and sharing state and configuration relevant to the tests.
    :param kafka: Kafka instance for interacting with the Kafka cluster.
    :param logger: Logger for recording information during the test process.
    :param topic: The topic where the messages will be sent by the producer and received by the consumer.
    :param num_records: Total number of records the producer should send.
    :param throughput: Target throughput for the producer, i.e., the number of messages per second.
    :return: None
    """
    producer = run_perf_producer(test_context=test_context, kafka=kafka, topic=topic, num_records=num_records, throughput=throughput)
    consumer = run_console_consumer(test_context=test_context, kafka=kafka, topic=topic)
    success, msg = validate_num(producer, consumer, logger)
    assert success, msg


def append_info(msg, boolean, cur_msg):
    """
    Appends additional information to a message based on a boolean condition.

    :param msg: The original message.
    :param boolean: The condition to check.
    :param cur_msg: The message to append if the condition is False.

    :return: The updated message.
    """
    if not boolean:
        msg += '\n' + cur_msg
    return msg


def capture_and_filter_logs(kafka, entry, log_path=None, start_time=None, end_time=None):
    """
    Capture and filter logs from a log file on a Kafka node.

    :param kafka: The Kafka cluster object containing node information.
    :type kafka: KafkaCluster
    :param log_path: The path to the log file.
    :type log_path: str
    :param entry: The log entry to search for.
    :type entry: str
    :param start_time: The start time for log filtering. Format should be compatible with awk. (optional)
    :type start_time: str, optional
    :param end_time: The end time for log filtering. Format should be compatible with awk. (optional)
    :type end_time: str, optional
    :return: A generator that yields lines from the filtered logs.
    :rtype: generator
    """
    if log_path is None:
        log_path = kafka.STDOUT_STDERR_CAPTURE
    if start_time and end_time:
        command = f"awk '$2>\"{start_time}\" && $2<\"{end_time}\"' {log_path} | grep '{entry}'"
    else:
        command = f"grep '{entry}' {log_path}"

    return kafka.nodes[0].account.ssh_capture(command)
