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
import os
import requests
import subprocess
import time
from typing import Dict, Any
import threading

from ducktape.mark import parametrize
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService
from kafkatest.services.external_services import DockerComposeService
from kafkatest.services.performance import AutoMQPerformanceService
from kafkatest.version import DEV_BRANCH
from kafkatest.services.trogdor.degraded_network_fault_spec import DegradedNetworkFaultSpec
from kafkatest.services.trogdor.trogdor import TrogdorService

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


class TableTopicE2ETest(Test):
    """
    End-to-end test for the Table Topic feature.
    Manages service lifecycle manually in setUp and tearDown to ensure correct startup order.
    """
    def __init__(self, test_context):
        super(TableTopicE2ETest, self).__init__(test_context)

        # These services will be managed manually in setUp and tearDown
        self.iceberg_catalog_service = None
        self.schema_registry_service = None

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
        # KafkaService is managed by ducktape automatically
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
            topics={}  # Topics will be created dynamically per test
        )

    def setUp(self):
        # Ducktape automatically starts self.kafka before setUp is called.
        # Now, we manually start the dependent services in the correct order.

        self.iceberg_catalog_service = DockerComposeService(ICEBERG_CATALOG_COMPOSE_PATH, self.logger)
        self.iceberg_catalog_service.start()

        self.kafka.start()
        bootstrap_servers = self.kafka.bootstrap_servers()
        schema_registry_env = {"KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers}
        self.schema_registry_service = DockerComposeService(SCHEMA_REGISTRY_COMPOSE_PATH, self.logger)
        self.schema_registry_service.start(env=schema_registry_env)

        # Health checks to ensure services are ready before the test method runs
        self._wait_for_service(ICEBERG_CATALOG_URL + "/v1/config", "Iceberg Catalog", 200)
        self._wait_for_service(SCHEMA_REGISTRY_URL, "Schema Registry")

    def tearDown(self):
        if self.schema_registry_service:
            self.schema_registry_service.stop()
        if self.iceberg_catalog_service:
            self.iceberg_catalog_service.stop()
        # Ducktape will automatically stop self.kafka after tearDown returns.

    def _wait_for_service(self, url, service_name, expected_status=200, timeout_sec=60):
        self.logger.info(f"Waiting for {service_name} to be available at {url}...")
        wait_until(
            lambda: self._check_service_health(url, expected_status),
            timeout_sec=timeout_sec,
            err_msg=f"{service_name} not ready after {timeout_sec} seconds."
        )

    def _check_service_health(self, url, expected_status):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == expected_status:
                self.logger.info(f"{url} is healthy.")
                return True
        except requests.exceptions.RequestException as e:
            self.logger.debug(f"Health check for {url} failed with exception: {e}")
        return False

    def _register_avro_schema(self, subject: str) -> int:
        """Register Avro schema and return schema ID"""
        self.logger.info(f"Registering Avro schema for subject '{subject}'...")
        schema_payload = {
            "schema": json.dumps(self.avro_schema)
        }
        response = requests.post(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
            json=schema_payload,
            headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'}
        )
        response.raise_for_status()
        schema_id = response.json()['id']
        self.logger.info(f"Registered Avro schema with ID: {schema_id}")
        return schema_id

    def _create_topic_with_config(self, topic_name: str, schema_type: str, commit_interval_ms: int = 2000):
        """Create topic with table topic configuration"""
        self.logger.info(f"Creating topic '{topic_name}' with schema type '{schema_type}'...")
        topic_config = {
            "topic": topic_name,
            "partitions": 16,
            "replication-factor": 1,
            "configs": {
                "automq.table.topic.enable": True,
                "automq.table.topic.commit.interval.ms": commit_interval_ms,
                "automq.table.topic.convert.value.type": "by_schema_id",
                "automq.table.topic.transform.value.type": "flatten",
                "automq.table.topic.namespace": "default"
            }
        }

        # Use KafkaService's built-in create_topic method
        self.kafka.create_topic(topic_config)
        self.logger.info(f"Created topic '{topic_name}' with config: {topic_config}")

    def _produce_avro_data_with_perf(self, topic_prefix: str, value_schema_json: str, target_msgs: int = 50,
                                     send_rate: int = 20, test_duration_seconds: int = None,
                                     commit_interval_ms: int = 2000) -> (str, int):
        """Produce Avro messages using AutoMQPerformanceService and return produced count via stable end offsets.

        We intentionally avoid using perf.results and instead wait for the perf run to complete, then
        poll Kafka end offsets until they stabilize to avoid counting while messages are still in-flight.
        """
        self.logger.info(f"Producing Avro messages to topics with prefix '{topic_prefix}' via AutoMQPerformanceService...")

        # Derive duration if not given (best-effort). Actual count will be read from perf result.
        if test_duration_seconds is None:
            # Ensure at least 1s and near target
            test_duration_seconds = max(1, int((target_msgs + send_rate - 1) // send_rate))

        perf = AutoMQPerformanceService(
            self.test_context, 1, kafka=self.kafka,
            producers_per_topic=1, groups_per_topic=0, consumers_per_group=1,
            topics=1, partitions_per_topic=16,
            send_rate=send_rate, record_size=256,
            topic_prefix=topic_prefix, await_topic_ready=False,
            topic_configs={
                # Ensure topic is created as TableTopic
                "automq.table.topic.enable": True,
                "automq.table.topic.commit.interval.ms": str(commit_interval_ms),
                "automq.table.topic.convert.value.type": "by_schema_id",
                "automq.table.topic.transform.value.type": "flatten",
                "automq.table.topic.namespace": ICEBERG_CATALOG_DB_NAME,
            },
            producer_configs={
                "schema.registry.url": SCHEMA_REGISTRY_URL,
                # Allow serializer to auto register schema if not present
                "auto.register.schemas": "true",
            },
            consumer_configs={},
            warmup_duration_minutes=0,
            test_duration_minutes=max(1, int((test_duration_seconds + 59) // 60)),
            value_schema=value_schema_json,
        )

        try:
            perf.run()
            # Reconstruct created topic name
            created_topic = self._perf_topic_name(topic_prefix, partitions=16, index=0)

            # Always use stable end offsets after perf completes
            produced = int(self._sum_kafka_end_offsets_stable(created_topic, settle_window_sec=2, timeout_sec=15))
            self.logger.info(f"Produced messages by stable end offsets: {produced} (target ~{target_msgs})")
            return created_topic, produced
        finally:
            # Ensure perf service is de-registered to avoid stop_all() warnings
            try:
                perf.free()
            except Exception as e:
                self.logger.warn(f"Failed to free AutoMQPerformanceService cleanly: {e}")

    def _perf_topic_name(self, prefix: str, partitions: int = 16, index: int = 0) -> str:
        return f"__automq_perf_{prefix}_{partitions:04d}_{index:07d}"

    def _sum_kafka_end_offsets(self, topic: str) -> int:
        """Sum log-end-offsets across all partitions for the given topic."""
        # Use KafkaService helper which invokes kafka-get-offsets.sh with --bootstrap-server
        output = self.kafka.get_offset_shell(time='-1', topic=topic)
        total = 0
        for raw in output.splitlines():
            line = raw.strip()
            # Expected format: <topic>:<partition>:<offset>
            parts = line.split(":")
            if len(parts) >= 3 and parts[0] == topic:
                try:
                    total += int(parts[-1])
                except Exception:
                    pass
        return total

    def _sum_kafka_end_offsets_stable(self, topic: str, settle_window_sec: int = 2, timeout_sec: int = 15) -> int:
        """Poll end offsets until they stabilize for settle_window_sec or timeout.

        This mitigates races where offsets are queried while producers are still flushing.
        """
        start = time.time()
        last = self._sum_kafka_end_offsets(topic)
        last_change = time.time()
        self.logger.info(f"Initial end offsets sum for '{topic}': {last}")
        while time.time() - start < timeout_sec:
            time.sleep(0.5)
            cur = self._sum_kafka_end_offsets(topic)
            if cur != last:
                self.logger.info(f"End offsets changed for '{topic}': {last} -> {cur}")
                last = cur
                last_change = time.time()
            else:
                if time.time() - last_change >= settle_window_sec:
                    self.logger.info(f"End offsets stabilized for '{topic}' at {cur}")
                    return cur
        self.logger.info(f"End offsets did not fully stabilize within {timeout_sec}s for '{topic}', using {last}")
        return last


    def _verify_data_in_iceberg(self, topic_name: str, expected_count: int, commit_interval_ms: int = 2000):
        """Verify Iceberg table exists and poll until snapshot shows expected_count or timeout.

        Robustly parses Iceberg metadata (supports both current-snapshot object and
        current-snapshot-id + snapshots) and logs detailed diagnostics for troubleshooting.
        """
        self.logger.info(f"Verifying data in Iceberg table '{topic_name}' with expected {expected_count} records...")

        table_url = f"{ICEBERG_CATALOG_URL}/v1/namespaces/{ICEBERG_CATALOG_DB_NAME}/tables/{topic_name}"
        snapshots_url = f"{table_url}/snapshots"

        headers = {
            'Accept': 'application/vnd.iceberg.v1+json, application/json'
        }

        timeout_sec = max(30, int(3 * commit_interval_ms / 1000))
        deadline = time.time() + timeout_sec

        last_status = None
        last_error = None
        last_record_count = None
        last_snapshot_id = None
        last_num_snapshots = None
        last_metadata_location = None

        def _parse_record_count(meta_obj: Dict[str, Any]) -> Dict[str, Any]:
            info = {
                'record_count': None,
                'snapshot_id': None,
                'num_snapshots': None,
                'metadata_location': None
            }
            if not isinstance(meta_obj, dict):
                return info

            metadata = meta_obj.get('metadata') or meta_obj
            info['metadata_location'] = (
                metadata.get('metadata-location') or
                metadata.get('metadataLocation') or
                meta_obj.get('metadata-location') or
                meta_obj.get('metadataLocation')
            )

            # Option A: embedded current-snapshot object
            current_snapshot = metadata.get('current-snapshot') or metadata.get('currentSnapshot')
            if isinstance(current_snapshot, dict):
                summary = current_snapshot.get('summary') or {}
                total = summary.get('total-records') or summary.get('totalRecords')
                try:
                    info['record_count'] = int(total) if total is not None else None
                except Exception:
                    info['record_count'] = None
                sid = current_snapshot.get('snapshot-id') or current_snapshot.get('snapshotId')
                info['snapshot_id'] = sid
                # snapshots count may not be present here
                return info

            # Option B: use current-snapshot-id and find snapshot in snapshots array
            current_snapshot_id = metadata.get('current-snapshot-id') or metadata.get('currentSnapshotId')
            snapshots = metadata.get('snapshots') or []
            info['num_snapshots'] = len(snapshots) if isinstance(snapshots, list) else None
            if current_snapshot_id and isinstance(snapshots, list):
                def _sid(x):
                    return x.get('snapshot-id') or x.get('snapshotId')
                for s in snapshots:
                    if _sid(s) == current_snapshot_id:
                        summary = s.get('summary') or {}
                        total = summary.get('total-records') or summary.get('totalRecords')
                        try:
                            info['record_count'] = int(total) if total is not None else None
                        except Exception:
                            info['record_count'] = None
                        info['snapshot_id'] = current_snapshot_id
                        return info

            # Fallback: take the last snapshot if available
            if isinstance(snapshots, list) and snapshots:
                s = snapshots[-1]
                summary = s.get('summary') or {}
                total = summary.get('total-records') or summary.get('totalRecords')
                try:
                    info['record_count'] = int(total) if total is not None else None
                except Exception:
                    info['record_count'] = None
                info['snapshot_id'] = s.get('snapshot-id') or s.get('snapshotId')
                return info

            return info

        while time.time() < deadline:
            try:
                resp = requests.get(table_url, headers=headers, timeout=5)
                last_status = resp.status_code
                if resp.status_code != 200:
                    body = None
                    try:
                        body = resp.text
                    except Exception:
                        body = '<no-body>'
                    self.logger.info(f"Iceberg GET {table_url} -> {resp.status_code}; body: {body[:256] if body else body}")
                    time.sleep(1)
                    continue

                meta = resp.json()
                parsed = _parse_record_count(meta)
                last_record_count = parsed['record_count']
                last_snapshot_id = parsed['snapshot_id']
                last_num_snapshots = parsed['num_snapshots']
                last_metadata_location = parsed['metadata_location']

                self.logger.info(
                    f"Iceberg table ok. metaLocation={last_metadata_location}, "
                    f"snapshots={last_num_snapshots}, currentSnapshot={last_snapshot_id}, "
                    f"total-records={last_record_count} (expect {expected_count})"
                )

                if last_record_count == expected_count:
                    break

                # If metadata didn’t include snapshots, try snapshots endpoint for extra clues
                if last_num_snapshots is None:
                    try:
                        sresp = requests.get(snapshots_url, headers=headers, timeout=5)
                        self.logger.info(f"Iceberg GET {snapshots_url} -> {sresp.status_code}")
                        if sresp.status_code == 200:
                            sdata = sresp.json()
                            if isinstance(sdata, dict) and 'snapshots' in sdata and isinstance(sdata['snapshots'], list):
                                self.logger.info(f"Snapshots listed: count={len(sdata['snapshots'])}")
                    except Exception as e:
                        self.logger.debug(f"Snapshots fetch error: {e}")

                time.sleep(1)
            except requests.exceptions.RequestException as e:
                last_error = str(e)
                self.logger.info(f"Error querying Iceberg table: {last_error}")
                time.sleep(1)

        if last_record_count != expected_count:
            msg = (
                f"Expected {expected_count} records, but found {last_record_count} in Iceberg after {timeout_sec}s. "
                f"status={last_status}, snapshotId={last_snapshot_id}, snapshots={last_num_snapshots}, "
                f"metaLocation={last_metadata_location}, lastError={last_error}. URL={table_url}"
            )
            self.logger.error(msg)
            assert False, msg

        self.logger.info("Data verification in Iceberg successful.")

    # ========== Chaos Utils ==========
    def _start_network_degrade_chaos(self, latency_ms: int = 50, duration_ms: int = 30000, device_name: str = "eth0"):
        """Start Trogdor network degrade chaos. Returns (trogdor_service, task). Caller must stop and wait_for_done."""
        trogdor = TrogdorService(context=self.test_context, client_services=[self.kafka])
        trogdor.start()
        spec = DegradedNetworkFaultSpec(0, duration_ms)
        for node in self.kafka.nodes:
            spec.add_node_spec(node.name, device_name, latencyMs=latency_ms, rateLimitKbit=0)
        task = trogdor.create_task(f"tt-net-degrade-{int(time.time())}", spec)
        return trogdor, task

    def _schedule_periodic_broker_kill_chaos(self, interval_secs: int = 300):
        """
        Periodically (every interval_secs) pick a random broker and do SIGKILL (unclean stop) followed by start.
        Returns (thread, stop_event). Caller should set stop_event when chaos period should end.
        """
        stop_event = threading.Event()

        def _loop():
            import random
            while not stop_event.is_set():
                try:
                    time.sleep(interval_secs)
                    node = random.choice(self.kafka.nodes)
                    self.logger.info(f"[Chaos] SIGKILL broker {node.account.hostname}")
                    # Unclean stop (SIGKILL)
                    self.kafka.stop_node(node, clean_shutdown=False, timeout_sec=60)
                    # Start back
                    self.kafka.start_node(node, timeout_sec=60)
                    self.logger.info(f"[Chaos] Broker {node.account.hostname} restarted after SIGKILL")
                except Exception as e:
                    self.logger.warn(f"[Chaos] Periodic broker kill failed: {e}")

        th = threading.Thread(target=_loop, daemon=True)
        th.start()
        return th, stop_event

    def _assert_no_broker_errors(self):
        """Scan broker operational logs for ERROR entries and fail if any found."""
        for node in self.kafka.nodes:
            log_dir = "/mnt/kafka/kafka-operational-logs/info"
            try:
                cmd = f"grep -R --line-number --ignore-case ' ERROR ' {log_dir} || true"
                out = "".join(list(node.account.ssh_capture(cmd)))
                # Filter benign lines if any. Keep simple for now
                if out.strip():
                    self.logger.error(f"Broker errors detected on {node.account.hostname}:\n{out}")
                    self.fail("Broker logs contain ERROR entries")
            except Exception as e:
                self.logger.warn(f"Failed to scan broker logs on {node.account.hostname}: {e}")

    def _verify_table_exists(self, topic_name: str) -> bool:
        """Simple check if table exists in Iceberg catalog"""
        try:
            table_url = f"{ICEBERG_CATALOG_URL}/v1/namespaces/{ICEBERG_CATALOG_DB_NAME}/tables/{topic_name}"
            response = requests.get(table_url)
            if response.status_code == 200:
                self.logger.info(f"Table '{topic_name}' exists in Iceberg catalog")
                return True
            else:
                self.logger.info(f"Table '{topic_name}' does not exist in Iceberg catalog (status: {response.status_code})")
                return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error checking table existence: {e}")
            return False

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
