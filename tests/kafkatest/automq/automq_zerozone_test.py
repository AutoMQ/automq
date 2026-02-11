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

from ducktape.mark.resource import cluster

from kafkatest.automq.automq_e2e_util import S3_WAL
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int


class AutoMQZeroZoneTest(ProduceConsumeValidateTest):
    """
    Verify producer/consumer traffic stays on the same rack broker when
    automq.zonerouter.channels is configured.
    """

    PRODUCER_CLIENT_ID = "automq_az=rack-a"
    CONSUMER_CLIENT_ID = "automq_az=rack-a"
    TOPIC = "automq_zerozone_topic"

    def __init__(self, test_context):
        super(AutoMQZeroZoneTest, self).__init__(test_context=test_context)
        self.kafka = None
        self.producer = None
        self.consumer = None
        self.num_producers = 1
        self.num_consumers = 1

    def min_cluster_size(self):
        return super(AutoMQZeroZoneTest, self).min_cluster_size() + self.num_producers * 2 + self.num_consumers * 2

    def setUp(self):
        self._create_kafka()
        self.kafka.start()

    def _create_kafka(self):
        server_prop_overrides = [
            ["automq.zonerouter.channels", S3_WAL],
        ]

        per_node_server_prop_overrides = {
            1: [("broker.rack", "rack-a")],
            2: [("broker.rack", "rack-b")],
            3: [("broker.rack", "rack-c")],
        }

        topics = {
            self.TOPIC: {
                "partitions": 10,
                "configs": {"min.insync.replicas": 1}
            }
        }

        self.kafka = KafkaService(
            self.test_context,
            num_nodes=3,
            zk=None,
            topics=topics,
            server_prop_overrides=server_prop_overrides,
            per_node_server_prop_overrides=per_node_server_prop_overrides,
        )

    def _producer_client_properties(self):
        return "request.timeout.ms=30000\nclient.id=%s\n" % self.PRODUCER_CLIENT_ID

    def _setup_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context,
            self.num_producers,
            self.kafka,
            self.TOPIC,
            throughput=1000,
            message_validator=is_int,
            client_prop_file_override=self._producer_client_properties()
        )
        self.consumer = ConsoleConsumer(
            self.test_context,
            self.num_consumers,
            self.kafka,
            self.TOPIC,
            client_id=self.CONSUMER_CLIENT_ID,
            group_id="automq-zerozone-group",
            consumer_timeout_ms=60000,
            message_validator=is_int
        )

    def _resolve_ip(self, from_node, hostname):
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", hostname):
            return hostname
        cmd = "getent hosts %s | awk '{print $1}' | head -n 1" % hostname
        ip = None
        for line in from_node.account.ssh_capture(cmd, allow_fail=True):
            line = line.strip()
            if line:
                ip = line
                break
        return ip or hostname

    def _broker_ips_for_node(self, node):
        return [self._resolve_ip(node, broker.account.hostname) for broker in self.kafka.nodes]

    def _assert_only_node1_traffic(self, node, label):
        broker_ips = self._broker_ips_for_node(node)
        node1_ip = broker_ips[0]
        other_broker_ips = set(broker_ips[1:])

        cmd = "iftop -t -s 10 -L 10 -n"
        iftop_output = [line for line in node.account.ssh_capture(cmd, allow_fail=False)]
        # The consumer still has some traffic to coordinator, so we set the threshold to 4096
        seen_ips = parse_iftop_ips(iftop_output, min_bps_bytes=4096)

        self.logger.info("%s iftop observed broker IPs: %s", label, sorted(seen_ips.intersection(set(broker_ips))))

        assert node1_ip in seen_ips, "%s: expected traffic to broker1 (%s)" % (label, node1_ip)
        assert not seen_ips.intersection(other_broker_ips), "%s: unexpected traffic to other brokers %s" % (
            label, sorted(other_broker_ips.intersection(seen_ips))
        )

    def _validate_zone_traffic(self):
        self._assert_only_node1_traffic(self.producer.nodes[0], "producer")
        self._assert_only_node1_traffic(self.consumer.nodes[0], "consumer")

    @cluster(num_nodes=5)
    def test_automq_zerozone(self):
        self._setup_producer_and_consumer()
        self.start_producer_and_consumer()
        # await producer & consumer to be stable.
        time.sleep(30)
        self._validate_zone_traffic()
        self.stop_producer_and_consumer()
        self.validate()


def rate_to_bytes_per_sec(value, unit):
    multiplier = 1
    if unit == "Kb":
        multiplier = 1024
    elif unit == "Mb":
        multiplier = 1024 ** 2
    elif unit == "Gb":
        multiplier = 1024 ** 3
    elif unit == "B":
        multiplier = 1
    return value * multiplier

def parse_iftop_ips(iftop_output, min_bps_bytes):
    ips = set()
    # 1 10.0.25.70                               =>      381Mb      403Mb      403Mb      504MB
    #  10.0.159.4                               <=      352Mb      428Mb      428Mb      535MB
    # 2 10.0.25.70                               =>      399Mb      387Mb      387Mb      484MB
    #  10.0.146.82                              <=      490Mb      441Mb      441Mb      552MB
    valid_line_count = 0
    traffic_between_two_nodes = 0
    traffic_line_re = re.compile(r"^\s*\d?\s(\d+\.\d+\.\d+\.\d+)\s+([<]=|=[>])\s+([\d.]+)([KMG]?b)")
    for line in iftop_output:
        m = traffic_line_re.match(line)
        if not m:
            continue
        ip = m.group(1)
        # Use the 'last 2s' column (groups 3 and 4)
        rate_val = float(m.group(3))
        rate_unit = m.group(4)
        rate_bps = rate_to_bytes_per_sec(rate_val, rate_unit)
        valid_line_count += 1
        traffic_between_two_nodes += rate_bps
        if valid_line_count % 2 == 0:
            if traffic_between_two_nodes >= min_bps_bytes:
                ips.add(ip)
                traffic_between_two_nodes = 0
    return ips