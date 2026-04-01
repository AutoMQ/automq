ARG jdk_version=eclipse-temurin:17-jdk-jammy
FROM $jdk_version

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    curl jq iproute2 iptables iputils-ping net-tools wget \
    && wget -O /usr/local/bin/jmxterm.jar https://github.com/jiaqi/jmxterm/releases/download/v1.0.4/jmxterm-1.0.4-uber.jar \
    && mkdir -p /opt/arthas && wget -O /opt/arthas/arthas-boot.jar https://arthas.aliyun.com/arthas-boot.jar \
    && apt-get -y clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/automq
CMD tail -f /dev/null
