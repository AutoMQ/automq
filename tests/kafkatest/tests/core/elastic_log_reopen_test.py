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

from kafkatest.directory_layout.kafka_path import CORE_LIBS_JAR_NAME, CORE_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.services.elastic_log_reopen_tester import ElasticLogReopenTester
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.version import DEV_BRANCH


class ElasticLogReOpenTest(Test):
    """
    This test verifies that partition can be reassigned and elastic log can be quickly reopened.
    """

    def __init__(self, test_context):
        super(ElasticLogReOpenTest, self).__init__(test_context)

        quorum_size_arg_name = 'quorum_size'
        default_quorum_size = 1
        quorum_size = default_quorum_size if not test_context.injected_args else test_context.injected_args.get(
            quorum_size_arg_name, default_quorum_size)
        if quorum_size < 1:
            raise Exception("Illegal %s value provided for the test: %s" % (quorum_size_arg_name, quorum_size))

        # start one controller node and two broker nodes for "remote_kraft" mode;
        # start one "broker,controller" node and one broker node for "colocated_kraft" mode.
        self.kafka = KafkaService(test_context, num_nodes=2, zk=None, controller_num_nodes_override=quorum_size)

    def setUp(self):
        self.kafka.start()

    @cluster(num_nodes=3)
    @matrix(topic_count=[1], partition_count=[1], replication_factor=[1], metadata_quorum=[quorum.all_kraft])
    def test_elastic_log_create(self, topic_count, partition_count, replication_factor,
                                metadata_quorum=quorum.colocated_kraft):
        """ Test that we can create a topic with elastic log and record time cost"""

        topics_create_start_time = time.time_ns()
        for i in range(topic_count):
            topic = "topic-new-%04d" % i
            print("Creating topic %s" % topic, flush=True)  # Force some stdout for Jenkins
            topic_cfg = {
                "topic": topic,
                "partitions": partition_count,
                "replication-factor": replication_factor
            }
            self.kafka.create_topic(topic_cfg)
        topics_create_end_time = time.time_ns()
        self.logger.info("Time to create topics: %.2f ms" % ((topics_create_end_time - topics_create_start_time) / 1e6))

    def start_test_log_compaction_tool(self, topic_name, partition_count=1, replication_factor=1):
        self.reopen_verifier = ElasticLogReopenTester(self.test_context, self.kafka, topic_name, partition_count, replication_factor)
        self.reopen_verifier.start()


    @cluster(num_nodes=4)
    @matrix(partition_count=[1], replication_factor=[1], metadata_quorum=[quorum.remote_kraft])
    def test_elastic_log_reassign(self, partition_count=1, replication_factor=1,
                                metadata_quorum=quorum.colocated_kraft):
        """ Test that we can reassign a topic with elastic log and record time cost"""

        topic = "test-elastic-log-reopen"
        self.start_test_log_compaction_tool(topic, partition_count, replication_factor)
        print(self.reopen_verifier.message)

