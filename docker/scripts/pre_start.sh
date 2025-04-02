#!/usr/bin/env bash

echo "[PreStart] mkdir"
rm -rf /opt/automq
mkdir -p /opt/kafka || exit 1
ln -s /opt/kafka /opt/automq || exit 1
echo "[PreStart] file"
for f in /opt/volume_libs/*.tgz; do
  tar -xzf "$f" -C /opt/kafka --one-top-level=kafka --strip-components=1 --overwrite
done
cp -r /opt/volume_scripts /opt/kafka/scripts || exit 1
find /opt/kafka -type f -name "*.sh" -exec chmod a+x {} \;
echo "[PreStart] env"
echo "export DEBIAN_FRONTEND=noninteractive" >> ~/.bashrc
echo "export AWS_DEFAULT_REGION=us-east-1" >> ~/.bashrc
echo "export KAFKA_JVM_PERFORMANCE_OPTS=\"-server -XX:+UseZGC -XX:ZCollectionInterval=5\"" >> ~/.bashrc
