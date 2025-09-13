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
from ducktape.mark import parametrize
from kafkatest.version import DEV_BRANCH
from .automq_tabletopic_base import TableTopicBase


class TableTopicMatrixTest(TableTopicBase):
    def __init__(self, test_context):
        super(TableTopicMatrixTest, self).__init__(test_context)

        # Simple Avro schema
        self.avro_schema = {
            "type": "record",
            "name": "MatrixAvroSchema",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "ts", "type": "long"}
            ]
        }
    # Use TableTopicBase helpers for perf, offsets, and Iceberg verification

    # Use TableTopicBase._run_perf

    @parametrize(commit_interval_ms=500)
    @parametrize(commit_interval_ms=2000)
    @parametrize(commit_interval_ms=10000)
    def test_commit_interval_matrix(self, commit_interval_ms, version=str(DEV_BRANCH)):
        topic_prefix = f"tt_ci_{commit_interval_ms}"
        topics = 1
        partitions = 16
        send_rate = 200
        duration_seconds = 60
        topic_configs = self.default_tabletopic_configs(commit_interval_ms)
        self._run_perf(topic_prefix, topics, partitions, send_rate, commit_interval_ms, duration_seconds,
                       value_schema_json=json.dumps(self.avro_schema),
                       topic_configs=topic_configs)
        # Single topic
        topic_name = self._perf_topic_name(topic_prefix, partitions, 0)
        produced = int(self._sum_kafka_end_offsets_stable(topic_name, 2, 15))
        self._verify_data_in_iceberg(topic_name, produced, commit_interval_ms=commit_interval_ms)

    @parametrize(topic_count=1)
    @parametrize(topic_count=10)
    @parametrize(topic_count=100)
    def test_topics_count_matrix(self, topic_count, version=str(DEV_BRANCH)):
        topic_prefix = f"tt_topics_{topic_count}"
        # Total 1000 partitions across all topics, total write rate 10000 across all partitions
        partitions = 1000 // topic_count  # Distribute total partitions across topics
        send_rate = (10000 // 1000) * partitions  # Rate per partition (10) * partitions per topic
        commit_interval_ms = 2000
        topic_configs = self.default_tabletopic_configs(commit_interval_ms)
        duration_seconds = 60
        self._run_perf(topic_prefix, topic_count, partitions, send_rate, commit_interval_ms, duration_seconds,
                        value_schema_json=json.dumps(self.avro_schema),
                       topic_configs=topic_configs)
        for i in range(topic_count):
            name = self._perf_topic_name(topic_prefix, partitions, i)
            produced = int(self._sum_kafka_end_offsets_stable(name, 2, 30))
            self._verify_data_in_iceberg(name, produced, commit_interval_ms=commit_interval_ms)
