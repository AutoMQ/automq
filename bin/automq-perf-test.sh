#!/bin/bash
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

base_dir=$(dirname "$0")

# The prefix of the topic used for catchup.
CATCHUP_TOPIC_PREFIX=""

while [[$# -gt 0 ]]; do
  case $1 in
    --catchup-topic-prefix)
      CATCHUP_TOPIC_PREFIX="$2"
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

NEW_ARGS=()
if [ -n "$CATCHUP_TOPIC_PREFIX"]; then
  NEW_ARGS+=("--catchup-topic-prefix" "$CATCHUP_TOPIC_PREFIX")
fi

if [ "x$KAFKA_LOG4J_OPTS" = "x" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/perf-log4j.properties"
fi

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx1024M"
fi

exec "$(dirname "$0")/kafka-run-class.sh" -name kafkaClient -loggc org.apache.kafka.tools.automq.PerfCommand "${NEW_ARGS[@]}" "$@"
