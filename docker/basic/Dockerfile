FROM docker.io/debian:bookworm

MAINTAINER AutoMQ for Apache Kafka dev@automq.com

ARG DEBIAN_MIRROR
RUN if [ ! -z "$DEBIAN_MIRROR" ]; then \
        sed -i "s/deb.debian.org/$DEBIAN_MIRROR/g" /etc/apt/sources.list.d/debian.sources; \
        fi

RUN apt-get update && apt-get install -y ca-certificates dnsutils iputils-ping curl wget netcat-openbsd sysstat net-tools htop procps zlib1g vim less openjdk-17-jdk libjemalloc-dev ipset iproute2 && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND=noninteractive \
    AWS_DEFAULT_REGION=us-east-1 \
    KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseZGC -XX:ZCollectionInterval=5"

RUN mkdir -p /data/kafka && mkdir -p /opt/kafka && ln -s /opt/kafka /opt/automq

WORKDIR /opt/kafka

CMD tail -f /dev/null
