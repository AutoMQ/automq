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
import textwrap

from ducktape.mark import cluster
from ducktape.utils.util import wait_until

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.connect import ConnectDistributedService, VerifiableSink, VerifiableSource


class ConnectAzMetadataTest(KafkaTest):
    """End-to-end validation that Connect honors AzMetadataProvider when building client configs."""

    AZ_CONFIG_KEY = "automq.test.az.id"
    EXPECTED_AZ = "test-az-1"
    PLUGIN_INSTALL_DIR = "/mnt/connect-az-plugin"
    TOPIC = "az-aware-connect"
    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    INPUT_FILE = "/mnt/connect.input"
    OUTPUT_FILE = "/mnt/connect.output"

    OFFSETS_TOPIC = "connect-offsets"
    OFFSETS_REPLICATION_FACTOR = "1"
    OFFSETS_PARTITIONS = "1"
    CONFIG_TOPIC = "connect-configs"
    CONFIG_REPLICATION_FACTOR = "1"
    STATUS_TOPIC = "connect-status"
    STATUS_REPLICATION_FACTOR = "1"
    STATUS_PARTITIONS = "1"
    EXACTLY_ONCE_SOURCE_SUPPORT = "disabled"
    SCHEDULED_REBALANCE_MAX_DELAY_MS = "60000"
    CONNECT_PROTOCOL="sessioned"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    FIRST_INPUT_LIST = ["foo", "bar", "baz"]
    FIRST_INPUTS = "\n".join(FIRST_INPUT_LIST) + "\n"
    SECOND_INPUT_LIST = ["razz", "ma", "tazz"]
    SECOND_INPUTS = "\n".join(SECOND_INPUT_LIST) + "\n"

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(ConnectAzMetadataTest, self).__init__(test_context, num_zk=1, num_brokers=1)
        # Single worker is sufficient; include plugin directory so it gets cleaned between runs
        self.cc = ConnectDistributedService(test_context, 1, self.kafka, [self.PLUGIN_INSTALL_DIR])
        self.source = None
        self.sink = None
        self._last_describe_output = ""

    @cluster(num_nodes=4)
    def test_consumer_metadata_contains_az(self):
        if self.zk:
            self.zk.start()
        self.kafka.start()

        self.cc.clean()
        self.PLUGIN_PATH = self.PLUGIN_INSTALL_DIR
        self._install_az_provider_plugin()

        self.cc.set_configs(lambda node: self._render_worker_config(node))
        self.cc.start()

        try:
            self.source = VerifiableSource(self.cc, topic=self.TOPIC, throughput=50)
            self.source.start()
            wait_until(lambda: len(self.source.sent_messages()) > 0, timeout_sec=60,
                       err_msg="Timed out waiting for VerifiableSource to emit records")

            self.sink = VerifiableSink(self.cc, topics=[self.TOPIC])
            self.sink.start()
            wait_until(lambda: len(self.sink.received_messages()) > 0, timeout_sec=60,
                       err_msg="Timed out waiting for VerifiableSink to consume records")

            group_id = "connect-%s" % self.sink.name

            def az_metadata_present():
                output = self.kafka.describe_consumer_group(group_id)
                self._last_describe_output = output
                return self._consumer_group_has_expected_metadata(output)

            wait_until(az_metadata_present, timeout_sec=60,
                       err_msg="Consumer group metadata never reflected AZ-aware client settings")

            assert self._consumer_group_has_expected_metadata(self._last_describe_output), \
                "Final consumer group output did not contain expected AZ metadata: %s" % self._last_describe_output
        finally:
            if self.sink is not None:
                self.sink.stop()
            if self.source is not None:
                self.source.stop()
            self.cc.stop()

    def _render_worker_config(self, node):
        base_config = self.render("connect-distributed.properties", node=node)
        # Ensure the worker passes the AZ hint down to the ServiceLoader plugin
        return base_config + "\n%s=%s\n" % (self.AZ_CONFIG_KEY, self.EXPECTED_AZ)

    def _install_az_provider_plugin(self):
        java_source = textwrap.dedent(f"""
            package org.apache.kafka.connect.automq.test;

            import java.util.Map;
            import java.util.Optional;
            import org.apache.kafka.connect.automq.AzMetadataProvider;

            public class FixedAzMetadataProvider implements AzMetadataProvider {
                private String az;

                @Override
                public void configure(Map<String, String> workerProps) {
                    this.az = workerProps.getOrDefault("{self.AZ_CONFIG_KEY}", null);
                    if (this.az != null && this.az.isBlank()) {
                        this.az = null;
                    }
                }

                @Override
                public Optional<String> availabilityZoneId() {
                    return Optional.ofNullable(this.az);
                }
            }
        """)
        service_definition = "org.apache.kafka.connect.automq.test.FixedAzMetadataProvider\n"
        classpath = "%s/libs/*" % self.cc.path.home()

        for node in self.cc.nodes:
            node.account.ssh("rm -rf {dir} && mkdir -p {dir}/org/apache/kafka/connect/automq/test".format(
                dir=self.PLUGIN_INSTALL_DIR
            ))
            node.account.ssh("mkdir -p {dir}/META-INF/services".format(dir=self.PLUGIN_INSTALL_DIR))
            java_path = "{dir}/org/apache/kafka/connect/automq/test/FixedAzMetadataProvider.java".format(
                dir=self.PLUGIN_INSTALL_DIR)
            service_path = "{dir}/META-INF/services/org.apache.kafka.connect.automq.AzMetadataProvider".format(
                dir=self.PLUGIN_INSTALL_DIR)
            node.account.create_file(java_path, java_source)
            node.account.create_file(service_path, service_definition)
            compile_cmd = "cd {dir} && javac -cp \"{cp}\" org/apache/kafka/connect/automq/test/FixedAzMetadataProvider.java".format(
                dir=self.PLUGIN_INSTALL_DIR, cp=classpath)
            node.account.ssh(compile_cmd)
            jar_cmd = "cd {dir} && jar cf az-provider.jar org META-INF".format(dir=self.PLUGIN_INSTALL_DIR)
            node.account.ssh(jar_cmd)

    def _consumer_group_has_expected_metadata(self, describe_output):
        lines = [line.strip() for line in describe_output.splitlines() if line.strip()]
        header_tokens = None
        header_index = {}
        for line in lines:
            upper = line.upper()
            if upper.startswith("TOPIC") and "CLIENT-ID" in upper:
                header_tokens = re.split(r"\s+", line)
                header_index = {token: idx for idx, token in enumerate(header_tokens)}
                continue
            if not header_tokens:
                continue
            if not line.startswith(self.TOPIC):
                continue
            tokens = re.split(r"\s+", line)
            if "CLIENT-ID" not in header_index or header_index["CLIENT-ID"] >= len(tokens):
                continue
            client_id = tokens[header_index["CLIENT-ID"]]
            if f"automq_az={self.EXPECTED_AZ}" not in client_id:
                continue
            if "CLIENT-RACK" not in header_index or header_index["CLIENT-RACK"] >= len(tokens):
                continue
            client_rack = tokens[header_index["CLIENT-RACK"]]
            if client_rack != self.EXPECTED_AZ:
                continue
            return True
        return False
