###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

FROM amazoncorretto:17-alpine

# exposed ports
EXPOSE 9092

USER root

# Get AutoMQ URL from https://github.com/AutoMQ/automq/releases and pass the url passed through build arguments
ARG kafka_url
ARG build_date


LABEL org.label-schema.name="automq-kafka" \
      org.label-schema.description="AutoMQ for Kafka" \
      org.label-schema.build-date="${build_date}" \
      org.label-schema.vcs-url="https://github.com/AutoMQ/automq" \
      maintainer="AutoMQ"

RUN set -eux ; \
    apk update ; \
    apk upgrade ; \
    apk add --no-cache wget gcompat procps bash jemalloc; \
    mkdir opt/kafka; \
    wget -nv -O automq.tgz "$kafka_url"; \
    tar xfz automq.tgz -C /opt/kafka --strip-components 1; \
    mkdir -p /var/lib/kafka/data /etc/kafka/secrets; \
    mkdir -p /etc/kafka/docker /usr/logs /mnt/shared/config; \
    adduser -h /home/appuser -D --shell /bin/bash appuser; \
    chown appuser:appuser -R /usr/logs /opt/kafka /mnt/shared/config; \
    chown appuser:root -R /var/lib/kafka /etc/kafka/secrets /etc/kafka; \
    chmod -R ug+w /etc/kafka /var/lib/kafka /etc/kafka/secrets; \
    cp /opt/kafka/config/log4j.properties /etc/kafka/docker/log4j.properties; \
    cp /opt/kafka/config/tools-log4j.properties /etc/kafka/docker/tools-log4j.properties; \
    cp /opt/kafka/config/kraft/reconfig-server.properties /etc/kafka/docker/server.properties; \
    rm automq.tgz; \
    apk del wget; \
    apk cache clean;

COPY --chown=appuser:appuser resources/common-scripts /etc/kafka/docker
COPY --chown=appuser:appuser launch /etc/kafka/docker/launch

# Configure jemalloc as the memory allocator
ENV LD_PRELOAD="/usr/lib/libjemalloc.so.2"

USER appuser

VOLUME ["/etc/kafka/secrets", "/var/lib/kafka/data", "/mnt/shared/config"]

CMD ["/etc/kafka/docker/run"]
