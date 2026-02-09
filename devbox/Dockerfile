ARG jdk_version=eclipse-temurin:17-jdk-jammy
FROM $jdk_version

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    curl jq iproute2 iputils-ping net-tools \
    && apt-get -y clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/automq
CMD tail -f /dev/null
