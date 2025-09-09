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

import os
import json
import time
from ducktape.utils.util import wait_until

from kafkatest.services.monitor.http import HttpMetricsCollector
from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH


class AutoMQPerformanceService(HttpMetricsCollector, PerformanceService):
    """
    Wrapper to run AutoMQ PerfCommand (bin/automq-perf-test.sh) from ducktape.
    Supports Avro via Schema Registry by passing --value-schema and optional --values-file.
    """

    PERSISTENT_ROOT = "/mnt/automq_perf"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "automq_perf.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "automq_perf.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "automq_perf.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")

    def __init__(self, context, num_nodes, kafka, version=DEV_BRANCH,
                 producers_per_topic=1, groups_per_topic=0, consumers_per_group=1,
                 topics=1, partitions_per_topic=1,
                 send_rate=1000, record_size=1024,
                 topic_prefix="tt", await_topic_ready=False,
                 topic_configs=None, producer_configs=None, consumer_configs=None,
                 test_duration_minutes=1, warmup_duration_minutes=0,
                 value_schema=None, values_file=None):
        super(AutoMQPerformanceService, self).__init__(context=context, num_nodes=num_nodes)

        self.logs = {
            "automq_perf_stdout": {
                "path": AutoMQPerformanceService.STDOUT_CAPTURE,
                "collect_default": True},
            "automq_perf_stderr": {
                "path": AutoMQPerformanceService.STDERR_CAPTURE,
                "collect_default": True},
            "automq_perf_log": {
                "path": AutoMQPerformanceService.LOG_FILE,
                "collect_default": True}
        }

        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()

        assert version.consumer_supports_bootstrap_server() or \
               self.security_config.security_protocol == SecurityConfig.PLAINTEXT

        self.params = {
            "producers_per_topic": producers_per_topic,
            "groups_per_topic": groups_per_topic,
            "consumers_per_group": consumers_per_group,
            "topics": topics,
            "partitions_per_topic": partitions_per_topic,
            "send_rate": send_rate,
            "record_size": record_size,
            "topic_prefix": topic_prefix,
            "await_topic_ready": await_topic_ready,
            "test_duration_minutes": test_duration_minutes,
            "warmup_duration_minutes": warmup_duration_minutes,
        }
        self.topic_configs = topic_configs or {}
        self.producer_configs = producer_configs or {}
        self.consumer_configs = consumer_configs or {}
        self.value_schema = value_schema
        self.values_file = values_file

        for node in self.nodes:
            node.version = version

    def start_cmd(self, node):
        bs = self.kafka.bootstrap_servers(self.security_config.security_protocol)
        script = self.path.script("automq-perf-test.sh", node)

        args = []
        args.append(f"-B {bs}")
        args.append(f"-p {self.params['producers_per_topic']}")
        args.append(f"-g {self.params['groups_per_topic']}")
        args.append(f"-c {self.params['consumers_per_group']}")
        args.append(f"-t {self.params['topics']}")
        args.append(f"-n {self.params['partitions_per_topic']}")
        args.append(f"-r {self.params['send_rate']}")
        args.append(f"-s {self.params['record_size']}")
        args.append(f"-X {self.params['topic_prefix']}")
        args.append(f"--await-topic-ready {str(self.params['await_topic_ready']).lower()}")
        args.append(f"-w {self.params['warmup_duration_minutes']}")
        args.append(f"-d {self.params['test_duration_minutes']}")

        # Topic/producer/consumer configs
        if self.topic_configs:
            topic_cfgs = " ".join([f"{k}={v}" for k, v in self.topic_configs.items()])
            args.append(f"-T {topic_cfgs}")
        if self.producer_configs:
            prod_cfgs = " ".join([f"{k}={v}" for k, v in self.producer_configs.items()])
            args.append(f"-P {prod_cfgs}")
        if self.consumer_configs:
            cons_cfgs = " ".join([f"{k}={v}" for k, v in self.consumer_configs.items()])
            args.append(f"-C {cons_cfgs}")

        # Avro schema options
        if self.value_schema:
            args.append(f"--value-schema '{self.value_schema}'")
            if self.values_file:
                args.append(f"--values-file {self.values_file}")

        cmd = f"{script} {' '.join(args)} 2>>{AutoMQPerformanceService.STDERR_CAPTURE} | tee {AutoMQPerformanceService.STDOUT_CAPTURE}"
        return cmd

    def _worker(self, idx, node):
        node.account.ssh(f"mkdir -p {AutoMQPerformanceService.PERSISTENT_ROOT}", allow_fail=False)

        cmd = self.start_cmd(node)
        self.logger.debug("AutoMQ perf command: %s", cmd)

        start = time.time()
        proc = node.account.ssh_capture(cmd)
        first_line = next(proc, None)
        if first_line is None:
            raise Exception("No output from AutoMQ performance command")

        # consume output until process exits
        for _ in proc:
            pass
        elapsed = time.time() - start
        self.logger.debug("AutoMQ PerfCommand ran for %s seconds" % elapsed)

        # Try to parse result file path and produced count
        result_file = None
        produced = None
        try:
            # Prefer explicit line if present in stdout
            for line in node.account.ssh_capture(f"cat {AutoMQPerformanceService.STDOUT_CAPTURE}"):
                if "Saving results to" in line:
                    result_file = line.strip().split("Saving results to")[-1].strip()
                    break
            # Fallback: find latest perf-*.json in CWD
            if not result_file:
                candidates = list(node.account.ssh_capture("ls -1t perf-*.json 2>/dev/null | head -1"))
                if candidates:
                    result_file = candidates[0].strip()
            if result_file:
                node.account.ssh(f"cp -f {result_file} {AutoMQPerformanceService.PERSISTENT_ROOT}/result.json || true", allow_fail=True)
                content = "".join(node.account.ssh_capture(f"cat {result_file}"))
                data = json.loads(content)
                produced = int(data.get("produceCountTotal", 0))
        except Exception:
            pass
        # record to results for test usage
        if self.results is not None and len(self.results) > 0:
            self.results[idx-1] = {"produced": produced, "result_file": result_file}

    def java_class_name(self):
        # Target the actual perf runner class to avoid killing unrelated Java processes
        # See bin/automq-perf-test.sh
        return "org.apache.kafka.tools.automq.PerfCommand"
