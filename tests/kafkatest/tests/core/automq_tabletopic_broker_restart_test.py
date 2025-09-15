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

import json
import random
import threading
import time

from .automq_tabletopic_base import TableTopicBase


class TableTopicBrokerRestartTest(TableTopicBase):
    def __init__(self, test_context):
        super(TableTopicBrokerRestartTest, self).__init__(test_context)
        self.avro_schema = {
            "type": "record",
            "name": "RestartAvroSchema",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "ts", "type": "long"}
            ]
        }

    def _restart_brokers_periodically(self, interval_secs: int, total_secs: int):
        stop_event = threading.Event()

        def loop():
            start = time.time()
            while not stop_event.is_set() and time.time() - start < total_secs:
                time.sleep(interval_secs)
                try:
                    node = random.choice(self.kafka.nodes)
                    self.logger.info(f"[PeriodicRestart] Restart broker {node.account.hostname}")
                    # Clean restart
                    self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=60)
                    self.kafka.start_node(node, timeout_sec=60)
                except Exception as e:
                    self.logger.warn(f"[PeriodicRestart] failed: {e}")

        th = threading.Thread(target=loop, daemon=True)
        th.start()
        return stop_event, th

    def test_broker_periodic_restart_10m(self):
        topic_prefix = "tt_restart"
        topics = 1
        partitions = 16
        send_rate = 200
        commit_interval_ms = 2000
        test_duration_seconds = 120  # 10 minutes

        # Start periodic restarts every 30s
        stop_event, th = self._restart_brokers_periodically(interval_secs=30, total_secs=test_duration_seconds)

        topic_configs = self.default_tabletopic_configs(commit_interval_ms)

        try:
            # Run perf using base helper (blocks for duration)
            self._run_perf(topic_prefix, topics, partitions, send_rate,
                           commit_interval_ms, test_duration_seconds,
                           value_schema_json=json.dumps(self.avro_schema),
                           topic_configs=topic_configs)
        finally:
            # Stop periodic restarts
            stop_event.set()
            th.join(timeout=10)

        topic_name = self._perf_topic_name(topic_prefix, partitions, 0)
        produced = int(self._sum_kafka_end_offsets_stable(topic_name, 3, 60))
        self._verify_data_in_iceberg(topic_name, produced, commit_interval_ms=commit_interval_ms)
