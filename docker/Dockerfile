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

FROM docker.io/debian:bookworm


MAINTAINER AutoMQ for Apache Kafka dev@automq.com

RUN apt-get update && apt-get install -y ca-certificates dnsutils iputils-ping curl wget netcat-openbsd sysstat net-tools htop procps zlib1g vim less openjdk-17-jdk libjemalloc-dev ipset iproute2 && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND=noninteractive \
    AWS_DEFAULT_REGION=us-east-1 \
    KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseZGC -XX:ZCollectionInterval=5"

COPY "./automq-*.tgz" /opt/kafka/kafka.tgz
RUN tar -xzvf "/opt/kafka/kafka.tgz" -C /opt/kafka/ --one-top-level=kafka --strip-components 1 && rm "/opt/kafka/kafka.tgz"
COPY scripts /opt/kafka/scripts
RUN chmod +x /opt/kafka/scripts/*.sh

RUN mkdir -p /data/kafka

RUN ln -s /opt/kafka /opt/automq
WORKDIR /opt/kafka

CMD tail -f /dev/null
