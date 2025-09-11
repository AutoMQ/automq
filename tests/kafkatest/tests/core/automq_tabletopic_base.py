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
import os
import time
from typing import Dict, Any

import requests
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.external_services import DockerComposeService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import AutoMQPerformanceService


# Static IP configuration from ducknet
SCHEMA_REGISTRY_HOST = "10.5.1.3"
SCHEMA_REGISTRY_PORT = 8081
SCHEMA_REGISTRY_URL = f"http://{SCHEMA_REGISTRY_HOST}:{SCHEMA_REGISTRY_PORT}"

ICEBERG_CATALOG_HOST = "10.5.1.4"
ICEBERG_CATALOG_PORT = 8181
ICEBERG_CATALOG_URL = f"http://{ICEBERG_CATALOG_HOST}:{ICEBERG_CATALOG_PORT}"
ICEBERG_CATALOG_S3_BUCKET = "iceberg-bucket"
ICEBERG_CATALOG_DB_NAME = "default"

# Docker compose file paths
DOCKER_COMPOSE_BASE_PATH = "/opt/kafka-dev/tests/docker"
SCHEMA_REGISTRY_COMPOSE_PATH = os.path.join(DOCKER_COMPOSE_BASE_PATH, "schema-registry/docker-compose.yaml")
ICEBERG_CATALOG_COMPOSE_PATH = os.path.join(DOCKER_COMPOSE_BASE_PATH, "iceberg-catalog/docker-compose.yaml")


class TableTopicBase(Test):
    """Common base for TableTopic tests: handles service lifecycle and shared helpers."""

    def __init__(self, test_context):
        super(TableTopicBase, self).__init__(test_context)
        self.iceberg_catalog_service = None
        self.schema_registry_service = None

        self.kafka = KafkaService(
            test_context,
            num_nodes=3,
            zk=None,
            server_prop_overrides=[
                ["automq.table.topic.catalog.type", "rest"],
                ["automq.table.topic.catalog.uri", ICEBERG_CATALOG_URL],
                ["automq.table.topic.catalog.warehouse", f"s3://{ICEBERG_CATALOG_S3_BUCKET}/wh/"],
                ["automq.table.topic.schema.registry.url", SCHEMA_REGISTRY_URL]
            ],
            topics={}
        )

    def setUp(self):
        self.iceberg_catalog_service = DockerComposeService(ICEBERG_CATALOG_COMPOSE_PATH, self.logger)
        self.iceberg_catalog_service.start()

        self.kafka.start()
        bootstrap_servers = self.kafka.bootstrap_servers()
        schema_registry_env = {"KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers}
        self.schema_registry_service = DockerComposeService(SCHEMA_REGISTRY_COMPOSE_PATH, self.logger)
        self.schema_registry_service.start(env=schema_registry_env)

        self._wait_for_service(ICEBERG_CATALOG_URL + "/v1/config", "Iceberg Catalog", 200)
        self._wait_for_service(SCHEMA_REGISTRY_URL, "Schema Registry")

    def tearDown(self):
        if self.schema_registry_service:
            self.schema_registry_service.stop()
        if self.iceberg_catalog_service:
            self.iceberg_catalog_service.stop()

    def _wait_for_service(self, url, service_name, expected_status=200, timeout_sec=60):
        self.logger.info(f"Waiting for {service_name} at {url}...")
        wait_until(lambda: self._ok(url, expected_status), timeout_sec=timeout_sec,
                   err_msg=f"{service_name} not ready in {timeout_sec}s")

    def _ok(self, url, expected_status):
        try:
            r = requests.get(url, timeout=5)
            return r.status_code == expected_status
        except Exception:
            return False

    def _perf_topic_name(self, prefix: str, partitions: int = 16, index: int = 0) -> str:
        return f"__automq_perf_{prefix}_{partitions:04d}_{index:07d}"

    def _sum_kafka_end_offsets(self, topic: str) -> int:
        output = self.kafka.get_offset_shell(time='-1', topic=topic)
        total = 0
        for raw in output.splitlines():
            line = raw.strip()
            parts = line.split(":")
            if len(parts) >= 3 and parts[0] == topic:
                try:
                    total += int(parts[-1])
                except Exception:
                    pass
        return total

    def _sum_kafka_end_offsets_stable(self, topic: str, settle_window_sec: int = 2, timeout_sec: int = 15) -> int:
        start = time.time()
        last = self._sum_kafka_end_offsets(topic)
        last_change = time.time()
        while time.time() - start < timeout_sec:
            time.sleep(0.5)
            cur = self._sum_kafka_end_offsets(topic)
            if cur != last:
                last = cur
                last_change = time.time()
            else:
                if time.time() - last_change >= settle_window_sec:
                    return cur
        return last

    def _verify_data_in_iceberg(self, topic_name: str, expected_count: int, commit_interval_ms: int = 2000):
        """Verify Iceberg table exists and poll until snapshot shows expected_count or timeout."""
        table_url = f"{ICEBERG_CATALOG_URL}/v1/namespaces/{ICEBERG_CATALOG_DB_NAME}/tables/{topic_name}"
        snapshots_url = f"{table_url}/snapshots"
        headers = {'Accept': 'application/vnd.iceberg.v1+json, application/json'}

        timeout_sec = max(30, int(3 * commit_interval_ms / 1000))
        deadline = time.time() + timeout_sec
        last_record_count = None
        last_status = None
        last_error = None
        last_snapshot_id = None
        last_num_snapshots = None
        last_metadata_location = None

        def parse(meta: Dict[str, Any]):
            info = {'record_count': None, 'snapshot_id': None, 'num_snapshots': None, 'metadata_location': None}
            if not isinstance(meta, dict):
                return info
            m = meta.get('metadata') or meta
            info['metadata_location'] = m.get('metadata-location') or m.get('metadataLocation') or \
                                        meta.get('metadata-location') or meta.get('metadataLocation')
            cur = m.get('current-snapshot') or m.get('currentSnapshot')
            if isinstance(cur, dict):
                s = cur.get('summary') or {}
                total = s.get('total-records') or s.get('totalRecords')
                try:
                    info['record_count'] = int(total) if total is not None else None
                except Exception:
                    info['record_count'] = None
                info['snapshot_id'] = cur.get('snapshot-id') or cur.get('snapshotId')
                return info
            cur_id = m.get('current-snapshot-id') or m.get('currentSnapshotId')
            snaps = m.get('snapshots') or []
            info['num_snapshots'] = len(snaps) if isinstance(snaps, list) else None
            if cur_id and isinstance(snaps, list):
                for s in snaps:
                    sid = s.get('snapshot-id') or s.get('snapshotId')
                    if sid == cur_id:
                        summ = s.get('summary') or {}
                        total = summ.get('total-records') or summ.get('totalRecords')
                        try:
                            info['record_count'] = int(total) if total is not None else None
                        except Exception:
                            info['record_count'] = None
                        info['snapshot_id'] = cur_id
                        return info
            if isinstance(snaps, list) and snaps:
                s = snaps[-1]
                summ = s.get('summary') or {}
                total = summ.get('total-records') or summ.get('totalRecords')
                try:
                    info['record_count'] = int(total) if total is not None else None
                except Exception:
                    info['record_count'] = None
                info['snapshot_id'] = s.get('snapshot-id') or s.get('snapshotId')
            return info

        while time.time() < deadline:
            try:
                resp = requests.get(table_url, headers=headers, timeout=5)
                last_status = resp.status_code
                if resp.status_code != 200:
                    time.sleep(1)
                    continue
                meta = resp.json()
                p = parse(meta)
                last_record_count = p['record_count']
                last_snapshot_id = p['snapshot_id']
                last_num_snapshots = p['num_snapshots']
                last_metadata_location = p['metadata_location']
                self.logger.info(
                    f"Iceberg table ok. metaLocation={last_metadata_location}, snapshots={last_num_snapshots}, "
                    f"currentSnapshot={last_snapshot_id}, total-records={last_record_count} (expect {expected_count})")
                if last_record_count == expected_count:
                    break
                # optional snapshots endpoint
                if last_num_snapshots is None:
                    try:
                        sresp = requests.get(snapshots_url, headers=headers, timeout=5)
                        self.logger.info(f"Iceberg GET {snapshots_url} -> {sresp.status_code}")
                    except Exception:
                        pass
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                last_error = str(e)
                time.sleep(1)

        if last_record_count != expected_count:
            msg = (
                f"Expected {expected_count} records, but found {last_record_count} in Iceberg after {timeout_sec}s. "
                f"status={last_status}, snapshotId={last_snapshot_id}, snapshots={last_num_snapshots}, "
                f"metaLocation={last_metadata_location}, lastError={last_error}. URL={table_url}"
            )
            self.logger.error(msg)
            assert False, msg

    def default_tabletopic_configs(self, commit_interval_ms: int, namespace: str = ICEBERG_CATALOG_DB_NAME) -> Dict[str, str]:
        return {
            "automq.table.topic.enable": "true",
            "automq.table.topic.commit.interval.ms": str(commit_interval_ms),
            "automq.table.topic.convert.value.type": "by_schema_id",
            "automq.table.topic.transform.value.type": "flatten",
            "automq.table.topic.namespace": namespace,
        }

    def _run_perf(self, topic_prefix: str, topics: int, partitions: int, send_rate: int,
                  commit_interval_ms: int, duration_seconds: int = None,
                  value_schema_json: str = None, topic_configs: Dict[str, str] = None):
        if duration_seconds is None:
            duration_seconds = 60
        perf = AutoMQPerformanceService(
            self.test_context, 1, kafka=self.kafka,
            producers_per_topic=1, groups_per_topic=0, consumers_per_group=1,
            topics=topics, partitions_per_topic=partitions,
            send_rate=send_rate, record_size=256,
            topic_prefix=topic_prefix, await_topic_ready=False,
            topic_configs=(topic_configs or {}),
            producer_configs={
                "schema.registry.url": SCHEMA_REGISTRY_URL,
                "auto.register.schemas": "true",
            },
            consumer_configs={},
            warmup_duration_minutes=0,
            test_duration_minutes=max(1, int((duration_seconds + 59) // 60)),
            value_schema=value_schema_json,
        )
        try:
            perf.run()
        finally:
            try:
                perf.stop()
            except Exception:
                pass
