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
import re
import time


def formatted_time(msg):
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
    force_use_zk_connection = False
    node = kafka.nodes[0]
    cmd = "%s --alter --add-config broker.quota.produce.bytes=%d,broker.quota.fetch.bytes=%d" % \
          (kafka.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection), producer_byte_rate,
           consumer_byte_rate)
    cmd += " --entity-type " + 'brokers'
    cmd += " --entity-name " + broker_id
    node.account.ssh(cmd)
