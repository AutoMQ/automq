#!/bin/bash
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

# shellcheck disable=SC1091

set -o errexit
set -o nounset
set -o pipefail
# set -o xtrace # Uncomment this line for debugging purposes

# Load libraries
. /opt/bitnami/scripts/libfs.sh
. /opt/bitnami/scripts/libos.sh
. /opt/bitnami/scripts/libkafka.sh

# Load Kafka environment variables
. /opt/bitnami/scripts/kafka-env.sh

# Map Kafka environment variables
kafka_create_alias_environment_variables

# Dinamically set node.id/broker.id/controller.quorum.voters if the _COMMAND environment variable is set
kafka_dynamic_environment_variables

# Set the default tuststore locations before validation
kafka_configure_default_truststore_locations
# Ensure Kafka user and group exist when running as 'root'
am_i_root && ensure_user_exists "$KAFKA_DAEMON_USER" --group "$KAFKA_DAEMON_GROUP"
# Ensure directories used by Kafka exist and have proper ownership and permissions
for dir in "$KAFKA_LOG_DIR" "$KAFKA_CONF_DIR" "$KAFKA_MOUNTED_CONF_DIR" "$KAFKA_VOLUME_DIR" "$KAFKA_DATA_DIR"; do
    if am_i_root; then
        ensure_dir_exists "$dir" "$KAFKA_DAEMON_USER" "$KAFKA_DAEMON_GROUP"
    else
        ensure_dir_exists "$dir"
    fi
done

# Kafka validation, skipped if server.properties was mounted at either $KAFKA_MOUNTED_CONF_DIR or $KAFKA_CONF_DIR
[[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/server.properties" && ! -f "$KAFKA_CONF_FILE" ]] && kafka_validate
# Kafka initialization, skipped if server.properties was mounted at $KAFKA_CONF_DIR
[[ ! -f "$KAFKA_CONF_FILE" ]] && kafka_initialize

# Initialise KRaft metadata storage if process.roles configured
if grep -q "^process.roles=" "$KAFKA_CONF_FILE" && ! is_boolean_yes "$KAFKA_SKIP_KRAFT_STORAGE_INIT" ; then
    kafka_kraft_storage_initialize
fi
# Configure Zookeeper SCRAM users
if is_boolean_yes "${KAFKA_ZOOKEEPER_BOOTSTRAP_SCRAM_USERS:-}"; then
    kafka_zookeeper_create_sasl_scram_users
fi
# KRaft controllers may get stuck starting when the controller quorum voters are changed.
# Workaround: Remove quorum-state file when scaling up/down controllers (Waiting proposal KIP-853)
# https://cwiki.apache.org/confluence/display/KAFKA/KIP-853%3A+KRaft+Voter+Changes
if [[ -f "${KAFKA_DATA_DIR}/__cluster_metadata-0/quorum-state" ]] && grep -q "^controller.quorum.voters=" "$KAFKA_CONF_FILE" && kafka_kraft_quorum_voters_changed; then
    warn "Detected inconsitences between controller.quorum.voters and quorum-state, removing it..."
    rm -f "${KAFKA_DATA_DIR}/__cluster_metadata-0/quorum-state"
fi
# Ensure custom initialization scripts are executed
kafka_custom_init_scripts
