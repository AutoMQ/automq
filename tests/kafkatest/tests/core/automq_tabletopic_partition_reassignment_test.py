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
import time

from ducktape.utils.util import wait_until
from .automq_tabletopic_base import TableTopicBase


class TableTopicPartitionReassignmentTest(TableTopicBase):
    def __init__(self, test_context):
        super(TableTopicPartitionReassignmentTest, self).__init__(test_context)

        self.avro_schema = {
            "type": "record",
            "name": "ReassignAvroSchema",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "ts", "type": "long"}
            ]
        }

    def _wait_topic_exists(self, topic_name: str, timeout_sec: int = 60):
        def exists():
            for t in self.kafka.list_topics():
                if t == topic_name:
                    return True
            return False
        wait_until(exists, timeout_sec=timeout_sec, err_msg=f"Topic {topic_name} not found in {timeout_sec}s")

    def _reassign_partitions(self, topic: str, num_partitions: int):
        desc = self.kafka.describe_topic(topic)
        info = self.kafka.parse_describe_topic(desc)
        self.logger.info(f"Before reassignment: {info}")
        # Shuffle partition mapping: swap partition ids while keeping replicas lists
        shuffled = list(range(0, num_partitions))
        random.shuffle(shuffled)
        for i in range(0, num_partitions):
            info["partitions"][i]["partition"] = shuffled[i]
        self.logger.info(f"Reassignment plan: {info}")
        # Execute and wait for completion
        self.kafka.execute_reassign_partitions(info)
        wait_until(lambda: self.kafka.verify_reassign_partitions(info), timeout_sec=120, backoff_sec=1,
                   err_msg="Partition reassignment did not complete in time")

    def test_partition_reassignment_during_produce(self):
        topic_prefix = "tt_reassign"
        partitions = 16
        send_rate = 200
        commit_interval_ms = 2000
        test_duration_seconds = 120

        topic_configs = self.default_tabletopic_configs(commit_interval_ms)

        try:
            # Kick off a delayed reassignment via a side thread
            def do_reassign():
                topic_name = self._perf_topic_name(topic_prefix, partitions, 0)
                self._wait_topic_exists(topic_name, 60)
                time.sleep(5)  # ensure leaders ready
                self._reassign_partitions(topic_name, partitions)

            import threading
            th = threading.Thread(target=lambda: (time.sleep(30), do_reassign()), daemon=True)
            th.start()

            # Run perf using base helper (blocks for duration)
            self._run_perf(topic_prefix, 1, partitions, send_rate,
                           commit_interval_ms, test_duration_seconds,
                           value_schema_json=json.dumps(self.avro_schema),
                           topic_configs=topic_configs)
            th.join(timeout=5)
        finally:
            pass

        topic_name = self._perf_topic_name(topic_prefix, partitions, 0)
        produced = int(self._sum_kafka_end_offsets_stable(topic_name, 3, 60))
        self._verify_data_in_iceberg(topic_name, produced, commit_interval_ms=commit_interval_ms)
