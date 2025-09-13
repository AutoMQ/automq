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
import time
from typing import Dict

from .automq_tabletopic_base import TableTopicBase

class TableTopicSchemaEvolutionTest(TableTopicBase):
    def __init__(self, test_context):
        super(TableTopicSchemaEvolutionTest, self).__init__(test_context)

        # Define 4 compatible schemas: each adds a field with a default value
        self.schemas = [
            {
                "type": "record",
                "name": "SchemaV1",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                ]
            },
            {
                "type": "record",
                "name": "SchemaV2",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string", "default": ""}
                ]
            },
            {
                "type": "record",
                "name": "SchemaV3",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string", "default": ""},
                    {"name": "age", "type": "int", "default": 0}
                ]
            },
            {
                "type": "record",
                "name": "SchemaV4",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string", "default": ""},
                    {"name": "age", "type": "int", "default": 0},
                    {"name": "country", "type": "string", "default": "cn"}
                ]
            }
        ]

    def _run_phase(self, topic_prefix: str, schema_json: str, partitions: int, send_rate: int,
                   commit_interval_ms: int, duration_seconds: int = 60, topic_configs: Dict[str, str] = None):
        # Allow caller to provide topic_configs; default to base helper if not provided
        effective_topic_configs = topic_configs if topic_configs is not None else self.default_tabletopic_configs(commit_interval_ms)
        self._run_perf(topic_prefix, 1, partitions, send_rate,
                       commit_interval_ms, duration_seconds,
                       value_schema_json=schema_json,
                       topic_configs=effective_topic_configs)

    def test_schema_evolution_4_phases(self):
        topic_prefix = "tt_schema_evo"
        partitions = 16
        send_rate = 200
        commit_interval_ms = 2000
        topic_configs = self.default_tabletopic_configs(commit_interval_ms)

        # Phase A: v1
        self._run_phase(topic_prefix, json.dumps(self.schemas[0]), partitions, send_rate, commit_interval_ms, 60, topic_configs=topic_configs)
        topic_name = self._perf_topic_name(topic_prefix, partitions, 0)
        produced_a = int(self._sum_kafka_end_offsets_stable(topic_name, 2, 30))
        self._verify_data_in_iceberg(topic_name, produced_a, commit_interval_ms)

        # Phase B: v2
        self._run_phase(topic_prefix, json.dumps(self.schemas[1]), partitions, send_rate, commit_interval_ms, 60, topic_configs=topic_configs)
        produced_b = int(self._sum_kafka_end_offsets_stable(topic_name, 2, 30))
        self._verify_data_in_iceberg(topic_name, produced_b, commit_interval_ms)

        # Phase C: v3
        self._run_phase(topic_prefix, json.dumps(self.schemas[2]), partitions, send_rate, commit_interval_ms, 60, topic_configs=topic_configs)
        produced_c = int(self._sum_kafka_end_offsets_stable(topic_name, 2, 30))
        self._verify_data_in_iceberg(topic_name, produced_c, commit_interval_ms)

        # Phase D: v4
        self._run_phase(topic_prefix, json.dumps(self.schemas[3]), partitions, send_rate, commit_interval_ms, 60, topic_configs=topic_configs)
        produced_d = int(self._sum_kafka_end_offsets_stable(topic_name, 2, 30))
        self._verify_data_in_iceberg(topic_name, produced_d, commit_interval_ms)
