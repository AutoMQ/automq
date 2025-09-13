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

import json

from ducktape.mark import parametrize
from .automq_tabletopic_base import TableTopicBase
from kafkatest.version import DEV_BRANCH


class TableTopicE2ETest(TableTopicBase):
    """
    End-to-end test for the Table Topic feature.
    Manages service lifecycle manually in setUp and tearDown to ensure correct startup order.
    """
    def __init__(self, test_context):
        super(TableTopicE2ETest, self).__init__(test_context)

        self.base_topic = "test_table_topic"

        # Avro schema definition
        self.avro_schema = {
            "type": "record",
            "name": "TestAvroSchema",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "timestamp", "type": "long"}
            ]
        }

    def _produce_avro_data_with_perf(self, topic_prefix: str, value_schema_json: str, target_msgs: int = 50,
                                     send_rate: int = 20, test_duration_seconds: int = None,
                                     commit_interval_ms: int = 2000) -> (str, int):
        if test_duration_seconds is None:
            test_duration_seconds = max(60, int((target_msgs + send_rate - 1) // send_rate))
        # Use base helper to run perf with standard TableTopic configs
        topic_configs = self.default_tabletopic_configs(commit_interval_ms)
        self._run_perf(topic_prefix, topics=1, partitions=16, send_rate=send_rate,
                       commit_interval_ms=commit_interval_ms, duration_seconds=test_duration_seconds,
                       value_schema_json=value_schema_json, topic_configs=topic_configs)
        created_topic = self._perf_topic_name(topic_prefix, partitions=16, index=0)
        produced_offsets = int(self._sum_kafka_end_offsets_stable(created_topic, settle_window_sec=2, timeout_sec=15))
        self.logger.info(f"Produced messages by offsets(stable): {produced_offsets} (target ~{target_msgs})")
        return created_topic, produced_offsets

    def _perf_topic_name(self, prefix: str, partitions: int = 16, index: int = 0) -> str:
        return f"__automq_perf_{prefix}_{partitions:04d}_{index:07d}"

    @parametrize(version=str(DEV_BRANCH))
    def test_tabletopic_avro_e2e_flow(self, version):
        """
        Tests the end-to-end flow of the table topic feature with Avro messages.
        """
        topic_name = f"{self.base_topic}_avro"
        target_msgs = 5000

        commit_interval_ms = 2000
        # Step 1: Produce Avro messages via AutoMQPerformanceService (random Avro if no values-file)
        created_topic, produced = self._produce_avro_data_with_perf(topic_name, json.dumps(self.avro_schema),
                                                                    target_msgs=target_msgs, commit_interval_ms=commit_interval_ms)
        # Step 2: Verify table exists and contains expected data (polling for commit interval)
        self._verify_data_in_iceberg(created_topic, produced, commit_interval_ms=commit_interval_ms)
