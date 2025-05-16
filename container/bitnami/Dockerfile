# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

FROM docker.io/bitnami/minideb:bookworm as extractor

COPY automq-*.tgz /tmp/
RUN mkdir -p /output && \
    tar -zxf /tmp/automq-*.tgz -C /output --strip-components=1

FROM docker.io/bitnami/minideb:bookworm

ARG DOWNLOADS_URL="downloads.bitnami.com/files/stacksmith"
ARG JAVA_EXTRA_SECURITY_DIR="/bitnami/java/extra-security"
ARG TARGETARCH

LABEL org.opencontainers.image.base.name="docker.io/bitnami/minideb:bookworm" \
      org.opencontainers.image.created="2025-04-27T21:51:40Z" \
      org.opencontainers.image.description="AutoMQ packaged by AutoMQ, Inc." \
      org.opencontainers.image.ref.name="3.9.0-debian-12-r13" \
      org.opencontainers.image.source="https://github.com/AutoMQ/automq/tree/main/container" \
      org.opencontainers.image.title="automq" \
      org.opencontainers.image.vendor="AutoMQ, Inc." \
      org.opencontainers.image.version="3.9.0"

ENV HOME="/" \
    OS_ARCH="${TARGETARCH:-amd64}" \
    OS_FLAVOUR="debian-12" \
    OS_NAME="linux"

COPY prebuildfs /
SHELL ["/bin/bash", "-o", "errexit", "-o", "nounset", "-o", "pipefail", "-c"]
# Install required system packages and dependencies
RUN install_packages ca-certificates curl procps zlib1g
RUN mkdir -p /tmp/bitnami/pkg/cache/ ; cd /tmp/bitnami/pkg/cache/ || exit 1 ; \
    COMPONENTS=( \
      "jre-17.0.14-10-1-linux-${OS_ARCH}-debian-12" \
    ) ; \
    for COMPONENT in "${COMPONENTS[@]}"; do \
      if [ ! -f "${COMPONENT}.tar.gz" ]; then \
        curl -SsLf "https://${DOWNLOADS_URL}/${COMPONENT}.tar.gz" -O ; \
        curl -SsLf "https://${DOWNLOADS_URL}/${COMPONENT}.tar.gz.sha256" -O ; \
      fi ; \
      sha256sum -c "${COMPONENT}.tar.gz.sha256" ; \
      tar -zxf "${COMPONENT}.tar.gz" -C /opt/bitnami --strip-components=2 --no-same-owner ; \
      rm -rf "${COMPONENT}".tar.gz{,.sha256} ; \
    done
COPY --from=extractor --chown=1001:0 /output /opt/bitnami/kafka
RUN apt-get update && apt-get install -y libjemalloc-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives
RUN chmod g+rwX /opt/bitnami
RUN find / -perm /6000 -type f -exec chmod a-s {} \; || true
RUN ln -s /opt/bitnami/scripts/kafka/entrypoint.sh /entrypoint.sh
RUN ln -s /opt/bitnami/scripts/kafka/run.sh /run.sh

COPY rootfs /
RUN /opt/bitnami/scripts/java/postunpack.sh
RUN /opt/bitnami/scripts/kafka/postunpack.sh
ENV APP_VERSION="3.9.0" \
    BITNAMI_APP_NAME="kafka" \
    JAVA_HOME="/opt/bitnami/java" \
    PATH="/opt/bitnami/common/bin:/opt/bitnami/java/bin:/opt/bitnami/kafka/bin:$PATH"

EXPOSE 9092

RUN arch=$(uname -m) \
    && target_file="/usr/lib/${arch}-linux-gnu/libjemalloc.so" \
    && { test -f "$target_file" || { echo "Error: $target_file not found"; exit 1; }; } \
    && ln -sv "$target_file" /usr/lib/libjemalloc.so
ENV LD_PRELOAD="/usr/lib/libjemalloc.so"

USER 1001
ENTRYPOINT [ "/opt/bitnami/scripts/kafka/entrypoint.sh" ]
CMD [ "/opt/bitnami/scripts/kafka/run.sh" ]
