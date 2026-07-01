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

# AutoMQ S3 Cost Estimator
#
# Estimates the monthly S3 cost of a running AutoMQ cluster and compares it
# to an equivalent Apache Kafka (EBS-backed) deployment cost.
#
# Usage:
#   bin/automq-cost-estimator.sh --bootstrap-server <HOST:PORT> [OPTIONS]
#
# Options:
#   -B, --bootstrap-server  HOST:PORT   Bootstrap server (required)
#   -c, --config            FILE        Admin client properties file
#       --storage-price     USD         S3 storage price per GiB/month (default: 0.023)
#       --put-price         USD         S3 PUT price per request (default: 0.000005)
#       --get-price         USD         S3 GET price per request (default: 0.0000004)
#       --ebs-price         USD         EBS gp3 price per GiB/month (default: 0.08)
#       --replication-factor  N         Kafka replication factor for comparison (default: 3)
#       --overprovision-factor MULT     Kafka EBS over-provision multiplier (default: 2.0)
#       --output            text|json   Output format (default: text)
#       --per-topic                     Show per-topic storage cost breakdown

exec "$(dirname "$0")/kafka-run-class.sh" \
  org.apache.kafka.tools.automq.S3CostEstimatorCommand "$@"
