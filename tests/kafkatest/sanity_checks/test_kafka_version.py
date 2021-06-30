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

from ducktape.tests.test import Test
from ducktape.mark.resource import cluster

from kafkatest.services.kafka import KafkaService, config_property
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.utils import is_version
from kafkatest.version import LATEST_0_8_2, DEV_BRANCH


class KafkaVersionTest(Test):
    """Sanity checks on kafka versioning."""
    def __init__(self, test_context):
        super(KafkaVersionTest, self).__init__(test_context)

        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)

    def setUp(self):
        self.zk.start()

    @cluster(num_nodes=2)
    def test_0_8_2(self):
        """Test kafka service node-versioning api - verify that we can bring up a single-node 0.8.2.X cluster."""
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        node = self.kafka.nodes[0]
        node.version = LATEST_0_8_2
        self.kafka.start()

        assert is_version(node, [LATEST_0_8_2], logger=self.logger)

    @cluster(num_nodes=3)
    def test_multi_version(self):
        """Test kafka service node-versioning api - ensure we can bring up a 2-node cluster, one on version 0.8.2.X,
        the other on the current development branch."""
        self.kafka = KafkaService(self.test_context, num_nodes=2, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 2}})
        # Be sure to make node[0] the one with v0.8.2 because the topic will be created using the --zookeeper option
        # since not all nodes support the --bootstrap-server option; the --zookeeper option is removed as of v3.0,
        # and the topic will be created against the broker on node[0], so that node has to be the one running the older
        # version (otherwise the kafka-topics --zookeeper command will fail).
        self.kafka.nodes[0].version = LATEST_0_8_2
        self.kafka.nodes[0].config[config_property.INTER_BROKER_PROTOCOL_VERSION] = "0.8.2.X"
        self.kafka.start()

        assert is_version(self.kafka.nodes[0], [LATEST_0_8_2], logger=self.logger)
        assert is_version(self.kafka.nodes[1], [DEV_BRANCH.vstring], logger=self.logger)
