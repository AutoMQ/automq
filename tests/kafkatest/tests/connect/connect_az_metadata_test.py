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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.connect import ConnectDistributedService, VerifiableSink, VerifiableSource


class ConnectAzMetadataTest(KafkaTest):
    """End-to-end validation that Connect honors AzMetadataProvider when building client configs."""

    AZ_CONFIG_KEY = "automq.test.az.id"
    EXPECTED_AZ = "test-az-1"
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
        # Single worker is sufficient for testing AZ metadata provider
        self.cc = ConnectDistributedService(test_context, 1, self.kafka, [])
        self.source = None
        self.sink = None
        self._last_describe_output = ""

    @cluster(num_nodes=4)
    def test_consumer_metadata_contains_az(self):
        if self.zk:
            self.zk.start()
        self.kafka.start()

        self.cc.clean()
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

            # Final verification that AZ metadata is present
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
        # Create a simple mock AzMetadataProvider implementation directly in the Connect runtime classpath
        java_source = textwrap.dedent("""
            package org.apache.kafka.connect.automq.test;

            import java.util.Map;
            import java.util.Optional;
            import org.apache.kafka.connect.automq.az.AzMetadataProvider;

            public class FixedAzMetadataProvider implements AzMetadataProvider {{
                private volatile Optional<String> availabilityZoneId = Optional.empty();

                @Override
                public void configure(Map<String, String> workerProps) {{
                    System.out.println("FixedAzMetadataProvider.configure() called with worker properties: " + workerProps.keySet());
                    
                    String az = workerProps.get("{}");
                    System.out.println("AZ config value for key '{}': " + az);
                    
                    if (az == null || az.isBlank()) {{
                        availabilityZoneId = Optional.empty();
                        System.out.println("FixedAzMetadataProvider: No AZ configured, setting to empty");
                    }} else {{
                        availabilityZoneId = Optional.of(az);
                        System.out.println("FixedAzMetadataProvider: Setting AZ to: " + az);
                    }}
                }}

                @Override
                public Optional<String> availabilityZoneId() {{
                    System.out.println("FixedAzMetadataProvider.availabilityZoneId() called, returning: " + availabilityZoneId.orElse("empty"));
                    return availabilityZoneId;
                }}
            }}
        """.format(self.AZ_CONFIG_KEY, self.AZ_CONFIG_KEY))
        
        service_definition = "org.apache.kafka.connect.automq.test.FixedAzMetadataProvider\n"
        
        for node in self.cc.nodes:
            # Get the Connect runtime classes directory where ServiceLoader will find our class
            kafka_home = self.cc.path.home()
            runtime_classes_dir = f"{kafka_home}/connect/runtime/build/classes/java/main"
            
            # Create the package directory structure in the runtime classes
            test_package_dir = f"{runtime_classes_dir}/org/apache/kafka/connect/automq/test"
            node.account.ssh(f"mkdir -p {test_package_dir}")
            node.account.ssh(f"mkdir -p {runtime_classes_dir}/META-INF/services")
            
            # Write the Java source file to a temporary location
            temp_src_dir = f"/tmp/az-provider-src/org/apache/kafka/connect/automq/test"
            node.account.ssh(f"mkdir -p {temp_src_dir}")
            java_path = f"{temp_src_dir}/FixedAzMetadataProvider.java"
            node.account.create_file(java_path, java_source)
            
            # Create the ServiceLoader service definition in the runtime classes
            service_path = f"{runtime_classes_dir}/META-INF/services/org.apache.kafka.connect.automq.az.AzMetadataProvider"
            node.account.create_file(service_path, service_definition)
            
            # Compile the Java file directly to the runtime classes directory
            classpath = f"{kafka_home}/connect/runtime/build/libs/*:{kafka_home}/connect/runtime/build/dependant-libs/*:{kafka_home}/clients/build/libs/*"
            compile_cmd = f"javac -cp \"{classpath}\" -d {runtime_classes_dir} {java_path}"
            print(f"Compiling with command: {compile_cmd}")
            result = node.account.ssh(compile_cmd, allow_fail=False)
            print(f"Compilation result: {result}")
            
            # Verify the compiled class exists in the runtime classes directory
            class_path = f"{test_package_dir}/FixedAzMetadataProvider.class"
            verify_cmd = f"ls -la {class_path}"
            verify_result = node.account.ssh(verify_cmd, allow_fail=True)
            print(f"Class file verification: {verify_result}")
            
            # Also verify the service definition exists
            service_verify_cmd = f"cat {service_path}"
            service_verify_result = node.account.ssh(service_verify_cmd, allow_fail=True)
            print(f"Service definition verification: {service_verify_result}")
            
            print(f"AZ metadata provider plugin installed in runtime classpath for node {node.account.hostname}")

    def _consumer_group_has_expected_metadata(self, describe_output):
        # Simply check if any line in the output contains our expected AZ metadata
        # This is more robust than trying to parse the exact table format
        expected_az_in_client_id = "automq_az={}".format(self.EXPECTED_AZ)
        
        # Debug: print the output to see what we're actually getting
        print("=== Consumer Group Describe Output ===")
        print(describe_output)
        print("=== Looking for: {} ===".format(expected_az_in_client_id))
        
        # Check if any line contains the expected AZ metadata
        for line in describe_output.splitlines():
            if expected_az_in_client_id in line:
                print("Found AZ metadata in line: {}".format(line))
                return True
        
        print("AZ metadata not found in consumer group output")
        return False
