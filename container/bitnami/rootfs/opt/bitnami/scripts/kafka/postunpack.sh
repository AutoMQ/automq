#!/bin/bash
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

# shellcheck disable=SC1091

set -o errexit
set -o nounset
set -o pipefail
# set -o xtrace # Uncomment this line for debugging purposes

# Load libraries
. /opt/bitnami/scripts/libkafka.sh
. /opt/bitnami/scripts/libfs.sh

# Load Kafka environment variables
. /opt/bitnami/scripts/kafka-env.sh

# Move server.properties from configtmp to config
# Temporary solution until kafka tarball places server.properties into config
if [[ -d "${KAFKA_BASE_DIR}/configtmp" ]]; then
    mv "${KAFKA_BASE_DIR}/configtmp"/* "$KAFKA_CONF_DIR"
    rmdir "${KAFKA_BASE_DIR}/configtmp"
fi
[[ -d "${KAFKA_BASE_DIR}/conf" ]] && rmdir "${KAFKA_BASE_DIR}/conf"

# Ensure directories used by Kafka exist and have proper ownership and permissions
for dir in "$KAFKA_LOG_DIR" "$KAFKA_CONF_DIR" "$KAFKA_MOUNTED_CONF_DIR" "$KAFKA_VOLUME_DIR" "$KAFKA_DATA_DIR" "$KAFKA_INITSCRIPTS_DIR"; do
    ensure_dir_exists "$dir"
done
chmod -R g+rwX "$KAFKA_BASE_DIR" "$KAFKA_VOLUME_DIR" "$KAFKA_DATA_DIR" "$KAFKA_INITSCRIPTS_DIR"

# Move the original server.properties, so users can skip initialization logic by mounting their own server.properties directly instead of using the MOUNTED_CONF_DIR
mv "${KAFKA_CONF_DIR}/server.properties" "${KAFKA_CONF_DIR}/server.properties.original"

# Disable logging to stdout and garbage collection
# Source: https://logging.apache.org/log4j/log4j-2.4/manual/appenders.html
replace_in_file "${KAFKA_BASE_DIR}/bin/kafka-server-start.sh" " [-]loggc" " "
replace_in_file "${KAFKA_CONF_DIR}/log4j.properties" "DailyRollingFileAppender" "ConsoleAppender"

# Disable the default console logger in favour of KafkaAppender (which provides the exact output)
echo "log4j.appender.stdout.Threshold=OFF" >>"${KAFKA_CONF_DIR}/log4j.properties"

# Remove invalid parameters for ConsoleAppender
remove_in_file "${KAFKA_CONF_DIR}/log4j.properties" "DatePattern"
remove_in_file "${KAFKA_CONF_DIR}/log4j.properties" "Appender.File"
