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

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin, CORE_LIBS_JAR_NAME, CORE_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.version import DEV_BRANCH


class ElasticLogReopenTester(KafkaPathResolverMixin, BackgroundThreadService):

    OUTPUT_DIR = "/mnt/elastic_log_reopen_tester"
    LOG_PATH = os.path.join(OUTPUT_DIR, "elastic_log_reopen_tester_stdout.log")
    VERIFICATION_STRING = "Reassigning partitions and reproducing took"

    logs = {
        "tool_logs": {
            "path": LOG_PATH,
            "collect_default": True}
    }

    def __init__(self, context, kafka, stop_timeout_sec=30):
        super(ElasticLogReopenTester, self).__init__(context, 1)
        self.kafka = kafka
        self.stop_timeout_sec = stop_timeout_sec
        self.reopen_test_completed = False

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % ElasticLogReopenTester.OUTPUT_DIR)
        cmd = self.start_cmd(node)
        self.logger.info("LogCompactionTester %d command: %s" % (idx, cmd))
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("Checking line:{}".format(line))

            if line.startswith(ElasticLogReopenTester.VERIFICATION_STRING):
                self.reopen_test_completed = True

    def start_cmd(self, node):
        core_libs_jar = self.path.jar(CORE_LIBS_JAR_NAME, DEV_BRANCH)
        core_dependant_test_libs_jar = self.path.jar(CORE_DEPENDANT_TEST_LIBS_JAR_NAME, DEV_BRANCH)

        cmd = "for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_libs_jar
        cmd += " for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_dependant_test_libs_jar
        cmd += " export CLASSPATH;"
        cmd += self.path.script("kafka-run-class.sh", node)
        cmd += " %s" % self.java_class_name()
        cmd += " --bootstrap-server %s --messages 1000000 --sleep 20 --duplicates 10 --percent-deletes 10" % (self.kafka.bootstrap_servers(self.security_protocol))

        cmd += " 2>> %s | tee -a %s &" % (self.logs["tool_logs"]["path"], self.logs["tool_logs"]["path"])
        return cmd

    def stop_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=True,
                                         allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False,
                                         allow_fail=True)
        node.account.ssh("rm -rf %s" % ElasticLogReopenTester.OUTPUT_DIR, allow_fail=False)

    def java_class_name(self):
        return "kafka.log.es.LogCompactionTester"

    @property
    def is_done(self):
        return self.reopen_test_completed