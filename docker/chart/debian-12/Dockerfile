# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

FROM docker.io/bitnami/minideb:bookworm AS extractor

ARG TARGETARCH

LABEL org.opencontainers.image.base.name="docker.io/bitnami/minideb:bookworm" \
      org.opencontainers.image.created="2025-04-27T21:51:40Z" \
      org.opencontainers.image.description="AutoMQ Enterprise packaged by AutoMQ, Inc." \
      org.opencontainers.image.licenses="BSL" \
      org.opencontainers.image.ref.name="3.7.0-debian-12-r6" \
      org.opencontainers.image.source="https://github.com/AutoMQ/automq/tree/main/container" \
      org.opencontainers.image.title="automq" \
      org.opencontainers.image.vendor="AutoMQ, Inc." \
      org.opencontainers.image.version="5.0.0"

ENV HOME="/" \
    OS_ARCH="${TARGETARCH:-amd64}" \
    OS_FLAVOUR="debian-12" \
    OS_NAME="linux"

# First copy rootfs to get the base structure
COPY debian-12/rootfs /

# Then extract automq package
COPY automq-*.tgz /tmp/pkg/cache/automq.tgz
RUN tar -zxf "/tmp/pkg/cache/automq.tgz" -C /opt/bitnami && \
    mv /opt/bitnami/automq-* /opt/bitnami/kafka && \
    chmod g+rwX /opt/bitnami

# Copy AutoMQ specific script
COPY debian-12/automq.sh /opt/bitnami/scripts/kafka/run.sh
RUN chmod +x /opt/bitnami/scripts/kafka/run.sh

FROM docker.io/bitnami/minideb:bookworm

ARG TARGETARCH

ENV HOME="/" \
    OS_ARCH="${TARGETARCH:-amd64}" \
    OS_FLAVOUR="debian-12" \
    OS_NAME="linux" \
    KAFKA_HEAP_OPTS="-Xmx512M"

COPY debian-12/prebuildfs /
SHELL ["/bin/bash", "-o", "errexit", "-o", "nounset", "-o", "pipefail", "-c"]

# Install required system packages and dependencies
RUN install_packages ca-certificates dnsutils iputils-ping curl wget netcat-openbsd sysstat net-tools telnet htop procps zlib1g vim less openjdk-17-jdk libjemalloc-dev ipset iproute2

# Copy the complete /opt/bitnami structure from extractor
COPY --chmod=0775 --from=extractor /opt/bitnami /opt/bitnami

RUN find / -perm /6000 -type f -exec chmod a-s {} \; || true

RUN arch=$(uname -m) \
    && target_file="/usr/lib/${arch}-linux-gnu/libjemalloc.so" \
    && { test -f "$target_file" || { echo "Error: $target_file not found"; exit 1; }; } \
    && ln -sv "$target_file" /usr/lib/libjemalloc.so

ENV LD_PRELOAD="/usr/lib/libjemalloc.so" \
    PATH="/opt/bitnami/kafka/bin:$PATH"

EXPOSE 9092

USER 1001
ENTRYPOINT [ "/opt/bitnami/scripts/kafka/entrypoint.sh" ]
CMD [ "/opt/bitnami/scripts/kafka/run.sh" ]
