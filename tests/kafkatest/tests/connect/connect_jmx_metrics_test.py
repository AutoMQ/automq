# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.tests.connect.connect_remote_write_test import ConnectRemoteWriteTest


class ConnectAutomaticJmxTest(ConnectRemoteWriteTest):
    """Verify that new connector tasks expose JMX metrics without changing Worker configuration."""

    @cluster(num_nodes=5)
    def test_automatic_jmx_metrics(self):
        """Discover, remove, and rediscover previously unknown connector MBeans in running Workers."""
        self.setup_services(num_workers=2)

        def worker_config(node):
            config = self.render("connect-distributed.properties", node=node)
            return config + "\nautomq.telemetry.exporter.interval.ms=1000\n"

        self.cc.set_configs(worker_config)
        self.cc.start()
        worker_pids = {node: self.cc.pids(node) for node in self.cc.nodes}

        for name in ("jmx-discovery-first", "jmx-discovery-second", "jmx-discovery-first"):
            self.cc.create_connector({
                "name": name,
                "connector.class": "org.apache.kafka.connect.tools.JmxMetricsSourceConnector",
                "tasks.max": "1",
                "topic": self.TOPIC,
                "throughput": "10"
            })
            wait_until(lambda: self._has_numeric_metric(name), timeout_sec=120,
                       err_msg="New connector JMX metrics were not discovered: " + name)
            self.cc.delete_connector(name)
            wait_until(lambda: not self._samples(name), timeout_sec=120,
                       err_msg="Deleted connector JMX metrics remained visible: " + name)

        for node in self.cc.nodes:
            assert self.cc.pids(node) == worker_pids[node], "JMX discovery must not restart Workers"

    def _samples(self, name):
        samples = []
        for node in self.cc.nodes:
            output = node.account.ssh_capture("curl -fsS http://localhost:9464/metrics")
            samples.extend(line.strip() for line in output
                           if line.startswith("connect_jmx_")
                           and 'jmx_domain="automq.connect.test"' in line
                           and 'jmx_attribute="RecordsPolled"' in line
                           and ('jmx_prop_connector="' + name + '"') in line)
        return samples

    def _has_numeric_metric(self, name):
        return any(float(line.split()[-1]) > 0 for line in self._samples(name))
