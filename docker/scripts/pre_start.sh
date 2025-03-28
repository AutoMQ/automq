#!/usr/bin/env bash

echo "[PreStart] shell check"
(hostname -I | awk '{print $1}') || exit 1
echo "[PreStart] mkdir"
rm -rf /opt/automq
mkdir -p /opt/kafka || exit 1
ln -s /opt/kafka /opt/automq || exit 1
echo "[PreStart] copy"
cp -r /opt/volume_libs /opt/kafka/kafka || exit 1
cp -r /opt/volume_scripts /opt/kafka/scripts || exit 1
echo "[PreStart] chmod"
find /opt/kafka -type f -name "*.sh" -exec chmod a+x {} \;
echo "[PreStart] env"
echo "export DEBIAN_FRONTEND=noninteractive" >> ~/.bashrc
echo "export AWS_DEFAULT_REGION=us-east-1" >> ~/.bashrc
echo "export KAFKA_JVM_PERFORMANCE_OPTS=\"-server -XX:+UseZGC -XX:ZCollectionInterval=5\"" >> ~/.bashrc
