#!/bin/bash
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0
#
# Bitnami Kafka library

# shellcheck disable=SC1090,SC1091

# Load Generic Libraries
. /opt/bitnami/scripts/libfile.sh
. /opt/bitnami/scripts/libfs.sh
. /opt/bitnami/scripts/liblog.sh
. /opt/bitnami/scripts/libos.sh
. /opt/bitnami/scripts/libvalidations.sh
. /opt/bitnami/scripts/libservice.sh

# Functions

########################
# Set a configuration setting value to a file
# Globals:
#   None
# Arguments:
#   $1 - file
#   $2 - key
#   $3 - values (array)
# Returns:
#   None
#########################
kafka_common_conf_set() {
    local file="${1:?missing file}"
    local key="${2:?missing key}"
    shift
    shift
    local values=("$@")

    if [[ "${#values[@]}" -eq 0 ]]; then
        stderr_print "missing value"
        return 1
    elif [[ "${#values[@]}" -ne 1 ]]; then
        for i in "${!values[@]}"; do
            kafka_common_conf_set "$file" "${key[$i]}" "${values[$i]}"
        done
    else
        value="${values[0]}"
        # Check if the value was set before
        if grep -q "^[#\\s]*$key\s*=.*" "$file"; then
            # Update the existing key
            replace_in_file "$file" "^[#\\s]*${key}\s*=.*" "${key}=${value}" false
        else
            # Add a new key
            printf '\n%s=%s' "$key" "$value" >>"$file"
        fi
    fi
}

########################
# Returns true if at least one listener is configured using SSL
# Globals:
#   KAFKA_CFG_LISTENERS
#   KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP
# Arguments:
#   None
# Returns:
#   true/false
#########################
kafka_has_ssl_listener(){
    if ! is_empty_value "${KAFKA_CFG_LISTENERS:-}"; then
        if is_empty_value "${KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP:-}"; then
            if [[ "$KAFKA_CFG_LISTENERS" =~ SSL: || "$KAFKA_CFG_LISTENERS" =~ SASL_SSL: ]]; then
                return
            fi
        else
            read -r -a protocol_maps <<<"$(tr ',' ' ' <<<"$KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP")"
            for protocol_map in "${protocol_maps[@]}"; do
                read -r -a map <<<"$(tr ':' ' ' <<<"$protocol_map")"
                # Obtain the listener and protocol from protocol map string, e.g. CONTROLLER:PLAINTEXT
                listener="${map[0]}"
                protocol="${map[1]}"
                if [[ "$protocol" = "SSL" || "$protocol" = "SASL_SSL" ]]; then
                    if [[ "$KAFKA_CFG_LISTENERS" =~ $listener ]]; then
                        return
                    fi
                fi
            done
        fi
    fi
    return 1
}

########################
# Returns true if at least one listener is configured using SASL
# Globals:
#   KAFKA_CFG_LISTENERS
#   KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP
# Arguments:
#   None
# Returns:
#   true/false
#########################
kafka_has_sasl_listener(){
    if ! is_empty_value "${KAFKA_CFG_LISTENERS:-}"; then
        if is_empty_value "${KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP:-}"; then
            if [[ "$KAFKA_CFG_LISTENERS" =~ SASL_PLAINTEXT: ]] || [[ "$KAFKA_CFG_LISTENERS" =~ SASL_SSL: ]]; then
                return
            fi
        else
            read -r -a protocol_maps <<<"$(tr ',' ' ' <<<"$KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP")"
            for protocol_map in "${protocol_maps[@]}"; do
                read -r -a map <<<"$(tr ':' ' ' <<<"$protocol_map")"
                # Obtain the listener and protocol from protocol map string, e.g. CONTROLLER:PLAINTEXT
                listener="${map[0]}"
                protocol="${map[1]}"
                if [[ "$protocol" = "SASL_PLAINTEXT" || "$protocol" = "SASL_SSL" ]]; then
                    if [[ "$KAFKA_CFG_LISTENERS" =~ $listener ]]; then
                        return
                    fi
                fi
            done
        fi
    fi
    return 1
}

########################
# Returns true if at least one listener is configured using plaintext
# Globals:
#   KAFKA_CFG_LISTENERS
#   KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP
# Arguments:
#   None
# Returns:
#   true/false
#########################
kafka_has_plaintext_listener(){
    if ! is_empty_value "${KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP:-}"; then
        read -r -a protocol_maps <<<"$(tr ',' ' ' <<<"$KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP")"
        for protocol_map in "${protocol_maps[@]}"; do
            read -r -a map <<<"$(tr ':' ' ' <<<"$protocol_map")"
            # Obtain the listener and protocol from protocol map string, e.g. CONTROLLER:PLAINTEXT
            listener="${map[0]}"
            protocol="${map[1]}"
            if [[ "$protocol" = "PLAINTEXT" ]]; then
                if is_empty_value "${KAFKA_CFG_LISTENERS:-}" || [[ "$KAFKA_CFG_LISTENERS" =~ $listener ]]; then
                    return
                fi
            fi
        done
    else
        if is_empty_value "${KAFKA_CFG_LISTENERS:-}" || [[ "$KAFKA_CFG_LISTENERS" =~ PLAINTEXT: ]]; then
            return
        fi
    fi
    return 1
}

########################
# Backwards compatibility measure to configure the TLS truststore locations
# Globals:
#   KAFKA_CONF_FILE
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_configure_default_truststore_locations() {
    # Backwards compatibility measure to allow custom truststore locations but at the same time not disrupt
    # the UX that the previous version of the containers and the helm chart have.
    # Context: The chart and containers by default assumed that the truststore location was KAFKA_CERTS_DIR/kafka.truststore.jks or KAFKA_MOUNTED_CONF_DIR/certs/kafka.truststore.jks.
    # Because of this, we could not use custom certificates in different locations (use case: A custom base image that already has a truststore). Changing the logic to allow custom
    # locations implied major changes in the current user experience (which only required to mount certificates at the assumed location). In order to maintain this compatibility we need
    # use this logic that sets the KAFKA_TLS_*_FILE variables to the previously assumed locations in case it is not set

    # Kafka truststore
    if kafka_has_ssl_listener && is_empty_value "${KAFKA_TLS_TRUSTSTORE_FILE:-}"; then
        local kafka_truststore_filename="kafka.truststore.jks"
        [[ "$KAFKA_TLS_TYPE" = "PEM" ]] && kafka_truststore_filename="kafka.truststore.pem"
        if [[ -f "${KAFKA_CERTS_DIR}/${kafka_truststore_filename}" ]]; then
            # Mounted in /opt/bitnami/kafka/conf/certs
            export KAFKA_TLS_TRUSTSTORE_FILE="${KAFKA_CERTS_DIR}/${kafka_truststore_filename}"
        else
            # Mounted in /bitnami/kafka/conf/certs
            export KAFKA_TLS_TRUSTSTORE_FILE="${KAFKA_MOUNTED_CONF_DIR}/certs/${kafka_truststore_filename}"
        fi
    fi
    # Zookeeper truststore
    if [[ "${KAFKA_ZOOKEEPER_PROTOCOL:-}" =~ SSL ]] && is_empty_value "${KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_FILE:-}"; then
        local zk_truststore_filename="zookeeper.truststore.jks"
        [[ "$KAFKA_ZOOKEEPER_TLS_TYPE" = "PEM" ]] && zk_truststore_filename="zookeeper.truststore.pem"
        if [[ -f "${KAFKA_CERTS_DIR}/${zk_truststore_filename}" ]]; then
            # Mounted in /opt/bitnami/kafka/conf/certs
            export KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_FILE="${KAFKA_CERTS_DIR}/${zk_truststore_filename}"
        else
            # Mounted in /bitnami/kafka/conf/certs
            export KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_FILE="${KAFKA_MOUNTED_CONF_DIR}/certs/${zk_truststore_filename}"
        fi
    fi
}

########################
# Set a configuration setting value to server.properties
# Globals:
#   KAFKA_CONF_FILE
# Arguments:
#   $1 - key
#   $2 - values (array)
# Returns:
#   None
#########################
kafka_server_conf_set() {
    kafka_common_conf_set "$KAFKA_CONF_FILE" "$@"
}

########################
# Set a configuration setting value to producer.properties and consumer.properties
# Globals:
#   KAFKA_CONF_DIR
# Arguments:
#   $1 - key
#   $2 - values (array)
# Returns:
#   None
#########################
kafka_producer_consumer_conf_set() {
    kafka_common_conf_set "$KAFKA_CONF_DIR/producer.properties" "$@"
    kafka_common_conf_set "$KAFKA_CONF_DIR/consumer.properties" "$@"
}

########################
# Create alias for environment variable, so both can be used
# Globals:
#   None
# Arguments:
#   $1 - Alias environment variable name
#   $2 - Original environment variable name
# Returns:
#   None
#########################
kafka_declare_alias_env() {
    local -r alias="${1:?missing environment variable alias}"
    local -r original="${2:?missing original environment variable}"
    if printenv "${original}" >/dev/null; then
        export "$alias"="${!original:-}"
    fi
}

########################
# Map Kafka legacy environment variables to the new names
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_create_alias_environment_variables() {
    suffixes=(
        "ADVERTISED_LISTENERS"
        "BROKER_ID"
        "NODE_ID"
        "CONTROLLER_QUORUM_VOTERS"
        "PROCESS_ROLES"
        "DEFAULT_REPLICATION_FACTOR"
        "DELETE_TOPIC_ENABLE"
        "INTER_BROKER_LISTENER_NAME"
        "LISTENERS"
        "LISTENER_SECURITY_PROTOCOL_MAP"
        "LOG_DIRS"
        "LOG_FLUSH_INTERVAL_MESSAGES"
        "LOG_FLUSH_INTERVAL_MS"
        "LOG_MESSAGE_FORMAT_VERSION"
        "LOG_RETENTION_BYTES"
        "LOG_RETENTION_CHECK_INTERVALS_MS"
        "LOG_RETENTION_HOURS"
        "LOG_SEGMENT_BYTES"
        "MESSAGE_MAX_BYTES"
        "NUM_IO_THREADS"
        "NUM_NETWORK_THREADS"
        "NUM_PARTITIONS"
        "NUM_RECOVERY_THREADS_PER_DATA_DIR"
        "OFFSETS_TOPIC_REPLICATION_FACTOR"
        "SOCKET_RECEIVE_BUFFER_BYTES"
        "SOCKET_REQUEST_MAX_BYTES"
        "SOCKET_SEND_BUFFER_BYTES"
        "SSL_ENDPOINT_IDENTIFICATION_ALGORITHM"
        "TRANSACTION_STATE_LOG_MIN_ISR"
        "TRANSACTION_STATE_LOG_REPLICATION_FACTOR"
        "ZOOKEEPER_CONNECT"
        "ZOOKEEPER_CONNECTION_TIMEOUT_MS"
    )
    kafka_declare_alias_env "KAFKA_CFG_LOG_DIRS" "KAFKA_LOGS_DIRS"
    kafka_declare_alias_env "KAFKA_CFG_LOG_SEGMENT_BYTES" "KAFKA_SEGMENT_BYTES"
    kafka_declare_alias_env "KAFKA_CFG_MESSAGE_MAX_BYTES" "KAFKA_MAX_MESSAGE_BYTES"
    kafka_declare_alias_env "KAFKA_CFG_ZOOKEEPER_CONNECTION_TIMEOUT_MS" "KAFKA_ZOOKEEPER_CONNECT_TIMEOUT_MS"
    kafka_declare_alias_env "KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE" "KAFKA_AUTO_CREATE_TOPICS_ENABLE"
    kafka_declare_alias_env "KAFKA_CLIENT_USERS" "KAFKA_BROKER_USER"
    kafka_declare_alias_env "KAFKA_CLIENT_PASSWORDS" "KAFKA_BROKER_PASSWORD"
    kafka_declare_alias_env "KAFKA_CLIENT_LISTENER_NAME" "KAFKA_CLIENT_LISTENER"
    for s in "${suffixes[@]}"; do
        kafka_declare_alias_env "KAFKA_CFG_${s}" "KAFKA_${s}"
    done
}

########################
# Validate settings in KAFKA_* env vars
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_validate() {
    debug "Validating settings in KAFKA_* env vars..."
    local error_code=0

    # Auxiliary functions
    print_validation_error() {
        error "$1"
        error_code=1
    }
    check_multi_value() {
        if [[ " ${2} " != *" ${!1} "* ]]; then
            print_validation_error "The allowed values for ${1} are: ${2}"
        fi
    }
    # If process.roles configured, check its values are valid and perform additional checks for each
    check_kraft_process_roles() {
        read -r -a roles_list <<<"$(tr ',;' ' ' <<<"$KAFKA_CFG_PROCESS_ROLES")"
        for role in "${roles_list[@]}"; do
            case "$role" in
            broker) ;;
            controller)
                if is_empty_value "${KAFKA_CFG_CONTROLLER_LISTENER_NAMES:-}"; then
                    print_validation_error "Role 'controller' enabled but environment variable KAFKA_CFG_CONTROLLER_LISTENER_NAMES was not provided."
                fi
                if is_empty_value "${KAFKA_CFG_LISTENERS:-}" || [[ ! "$KAFKA_CFG_LISTENERS" =~ ${KAFKA_CFG_CONTROLLER_LISTENER_NAMES} ]]; then
                    print_validation_error "Role 'controller' enabled but listener ${KAFKA_CFG_CONTROLLER_LISTENER_NAMES} not found in KAFKA_CFG_LISTENERS."
                fi
                ;;
            *)
                print_validation_error "Invalid KRaft process role '$role'. Supported roles are 'broker,controller'"
                ;;
            esac
        done
    }
    # Check all listeners are using a unique and valid port
    check_listener_ports(){
        check_allowed_port() {
            local port="${1:?missing port variable}"
            local -a validate_port_args=()
            ! am_i_root && validate_port_args+=("-unprivileged")
            validate_port_args+=("$port")
            if ! err=$(validate_port "${validate_port_args[@]}"); then
                print_validation_error "An invalid port ${port} was specified in the environment variable KAFKA_CFG_LISTENERS: ${err}."
            fi
        }

        read -r -a listeners <<<"$(tr ',' ' ' <<<"${KAFKA_CFG_LISTENERS:-}")"
        local -a ports=()
        for listener in "${listeners[@]}"; do
            read -r -a arr <<<"$(tr ':' ' ' <<<"$listener")"
            # Obtain the port from listener string, e.g. PLAINTEXT://:9092
            port="${arr[2]}"
            check_allowed_port "$port"
            ports+=("$port")
        done
        # Check each listener is using an unique port
        local -a unique_ports=()
        read -r -a unique_ports <<< "$(echo "${ports[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')"
        if [[ "${#ports[@]}" != "${#unique_ports[@]}" ]]; then
            print_validation_error "There are listeners bound to the same port"
        fi
    }
    check_listener_protocols(){
        local -r allowed_protocols=("PLAINTEXT" "SASL_PLAINTEXT" "SASL_SSL" "SSL")
        read -r -a protocol_maps <<<"$(tr ',' ' ' <<<"$KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP")"
        for protocol_map in "${protocol_maps[@]}"; do
            read -r -a map <<<"$(tr ':' ' ' <<<"$protocol_map")"
            # Obtain the listener and protocol from protocol map string, e.g. CONTROLLER:PLAINTEXT
            listener="${map[0]}"
            protocol="${map[1]}"
            # Check protocol in allowed list
            if [[ ! "${allowed_protocols[*]}" =~ $protocol ]]; then
                print_validation_error "Authentication protocol ${protocol} is not supported!"
            fi
            # If inter-broker listener configured with SASL, ensure KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL is set
            if [[ "$listener" = "${KAFKA_CFG_INTER_BROKER_LISTENER_NAME:-INTERNAL}" ]]; then
                if [[ "$protocol" = "SASL_PLAINTEXT" ]] || [[ "$protocol" = "SASL_SSL" ]]; then
                    if is_empty_value "${KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL:-}"; then
                        print_validation_error "When using SASL for inter broker comunication the mechanism should be provided using KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL"
                    fi
                    if is_empty_value "${KAFKA_INTER_BROKER_USER:-}" || is_empty_value "${KAFKA_INTER_BROKER_PASSWORD:-}"; then
                        print_validation_error "In order to configure SASL authentication for Kafka inter-broker communications, you must provide the SASL credentials. Set the environment variables KAFKA_INTER_BROKER_USER and KAFKA_INTER_BROKER_PASSWORD to configure the credentials for SASL authentication with between brokers."
                    fi
                fi
            # If controller listener configured with SASL, ensure KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL is set
            elif [[ "${KAFKA_CFG_CONTROLLER_LISTENER_NAMES:-CONTROLLER}" =~ $listener ]]; then
                if [[ "$protocol" = "SASL_PLAINTEXT" ]] || [[ "$protocol" = "SASL_SSL" ]]; then
                    if is_empty_value "${KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL:-}"; then
                        print_validation_error "When using SASL for controller comunication the mechanism should be provided at KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL"
                    elif [[ "$KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL" =~ SCRAM ]]; then
                        warn "KRaft controller listener may not support SCRAM-SHA-256/SCRAM-SHA-512 mechanisms. If facing any issues, we recommend switching to PLAIN mechanism. More information at: https://issues.apache.org/jira/browse/KAFKA-15513"
                    fi
                    if is_empty_value "${KAFKA_CONTROLLER_USER:-}" || is_empty_value "${KAFKA_CONTROLLER_PASSWORD:-}"; then
                        print_validation_error "In order to configure SASL authentication for Kafka control plane communications, you must provide the SASL credentials. Set the environment variables KAFKA_CONTROLLER_USER and KAFKA_CONTROLLER_PASSWORD to configure the credentials for SASL authentication with between controllers."
                    fi
                fi
            else
                if [[ "$protocol" = "SASL_PLAINTEXT" ]] || [[ "$protocol" = "SASL_SSL" ]]; then
                    if is_empty_value "${KAFKA_CLIENT_USERS:-}" || is_empty_value "${KAFKA_CLIENT_PASSWORDS:-}"; then
                        print_validation_error "In order to configure SASL authentication for Kafka, you must provide the SASL credentials. Set the environment variables KAFKA_CLIENT_USERS and KAFKA_CLIENT_PASSWORDS to configure the credentials for SASL authentication with clients."
                    fi
                fi

            fi
        done
    }

    if is_empty_value "${KAFKA_CFG_PROCESS_ROLES:-}" && is_empty_value "${KAFKA_CFG_ZOOKEEPER_CONNECT:-}"; then
        print_validation_error "Kafka haven't been configured to work in either Raft or Zookeper mode. Please make sure at least one of the modes is configured."
    fi
    # Check KRaft mode
    if ! is_empty_value "${KAFKA_CFG_PROCESS_ROLES:-}"; then
        # Only allow Zookeeper configuration if migration mode is enabled
        if ! is_empty_value "${KAFKA_CFG_ZOOKEEPER_CONNECT:-}" &&
            { is_empty_value "${KAFKA_CFG_ZOOKEEPER_METADATA_MIGRATION_ENABLE:-}" || ! is_boolean_yes "$KAFKA_CFG_ZOOKEEPER_METADATA_MIGRATION_ENABLE"; }; then
            print_validation_error "Both KRaft mode and Zookeeper modes are configured, but KAFKA_CFG_ZOOKEEPER_METADATA_MIGRATION_ENABLE is not enabled"
        fi
        if is_empty_value "${KAFKA_CFG_NODE_ID:-}"; then
            print_validation_error "KRaft mode requires an unique node.id, please set the environment variable KAFKA_CFG_NODE_ID"
        fi
        if is_empty_value "${KAFKA_CFG_CONTROLLER_QUORUM_VOTERS:-}"; then
            print_validation_error "KRaft mode requires KAFKA_CFG_CONTROLLER_QUORUM_VOTERS to be set"
        fi
        check_kraft_process_roles
    fi
    # Check Zookeeper mode
    if ! is_empty_value "${KAFKA_CFG_ZOOKEEPER_CONNECT:-}"; then
        # If SSL/SASL_SSL protocol configured, check certificates are provided
        if [[ "$KAFKA_ZOOKEEPER_PROTOCOL" =~ SSL ]]; then
            if [[ "$KAFKA_ZOOKEEPER_TLS_TYPE" = "JKS" ]]; then
                # Fail if truststore is not provided
                if [[ ! -f "$KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_FILE" ]]; then
                    print_validation_error "In order to configure the TLS encryption for Zookeeper with JKS certs you must mount your zookeeper.truststore.jks cert to the ${KAFKA_MOUNTED_CONF_DIR}/certs directory."
                fi
                # Warn if keystore is not provided, only required if Zookeper mTLS is enabled (ZOO_TLS_CLIENT_AUTH)
                if [[ ! -f "${KAFKA_CERTS_DIR}/zookeeper.keystore.jks" ]] && [[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/certs/zookeeper.keystore.jks" ]]; then
                    warn "In order to configure the mTLS for Zookeeper with JKS certs you must mount your zookeeper.keystore.jks cert to the ${KAFKA_MOUNTED_CONF_DIR}/certs directory."
                fi
            elif [[ "$KAFKA_ZOOKEEPER_TLS_TYPE" = "PEM" ]]; then
                # Fail if CA / validation cert is not provided
                if [[ ! -f "$KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_FILE" ]]; then
                    print_validation_error "In order to configure the TLS encryption for Zookeeper with PEM certs you must mount your zookeeper.truststore.pem cert to the ${KAFKA_MOUNTED_CONF_DIR}/certs directory."
                fi
                # Warn if node key or cert are not provided, only required if Zookeper mTLS is enabled (ZOO_TLS_CLIENT_AUTH)
                if { [[ ! -f "${KAFKA_CERTS_DIR}/zookeeper.keystore.pem" ]] || [[ ! -f "${KAFKA_CERTS_DIR}/zookeeper.keystore.key" ]]; } &&
                    { [[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/certs/zookeeper.keystore.pem" ]] || [[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/certs/zookeeper.keystore.key" ]]; }; then
                    warn "In order to configure the mTLS for Zookeeper with PEM certs you must mount your zookeeper.keystore.pem cert and zookeeper.keystore.key key to the ${KAFKA_MOUNTED_CONF_DIR}/certs directory."
                fi
            fi
        fi
        # If SASL/SASL_SSL protocol configured, check certificates are provided
        if [[ "$KAFKA_ZOOKEEPER_PROTOCOL" =~ SASL ]]; then
            if is_empty_value "${KAFKA_ZOOKEEPER_USER:-}" || is_empty_value "${KAFKA_ZOOKEEPER_PASSWORD:-}"; then
                print_validation_error "In order to configure SASL authentication for Kafka, you must provide the SASL credentials. Set the environment variables KAFKA_ZOOKEEPER_USER and KAFKA_ZOOKEEPER_PASSWORD, to configure the credentials for SASL authentication with Zookeeper."
            fi
        fi
        # If using plaintext protocol, check it is explicitly allowed
        if [[ "$KAFKA_ZOOKEEPER_PROTOCOL" = "PLAINTEXT" ]]; then
            warn "The KAFKA_ZOOKEEPER_PROTOCOL environment variable does not configure SASL and/or SSL, this setting is not recommended for production environments."
        fi
    fi
    # Check listener ports are unique and allowed
    check_listener_ports
    # Check listeners are mapped to a valid security protocol
    check_listener_protocols
    # Warn users if plaintext listeners are configured
    if kafka_has_plaintext_listener; then
        warn "Kafka has been configured with a PLAINTEXT listener, this setting is not recommended for production environments."
    fi
    # If SSL/SASL_SSL listeners configured, check certificates are provided
    if kafka_has_ssl_listener; then
        if [[ "$KAFKA_TLS_TYPE" = "JKS" ]] &&
            { [[ ! -f "${KAFKA_CERTS_DIR}/kafka.keystore.jks" ]] || [[ ! -f "$KAFKA_TLS_TRUSTSTORE_FILE" ]]; } &&
            { [[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/certs/kafka.keystore.jks" ]] || [[ ! -f "$KAFKA_TLS_TRUSTSTORE_FILE" ]]; }; then
            print_validation_error "In order to configure the TLS encryption for Kafka with JKS certs you must mount your kafka.keystore.jks and kafka.truststore.jks certs to the ${KAFKA_MOUNTED_CONF_DIR}/certs directory."
        elif [[ "$KAFKA_TLS_TYPE" = "PEM" ]] &&
            { [[ ! -f "${KAFKA_CERTS_DIR}/kafka.keystore.pem" ]] || [[ ! -f "${KAFKA_CERTS_DIR}/kafka.keystore.key" ]] || [[ ! -f "$KAFKA_TLS_TRUSTSTORE_FILE" ]]; } &&
            { [[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/certs/kafka.keystore.pem" ]] || [[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/certs/kafka.keystore.key" ]] || [[ ! -f "$KAFKA_TLS_TRUSTSTORE_FILE" ]]; }; then
            print_validation_error "In order to configure the TLS encryption for Kafka with PEM certs you must mount your kafka.keystore.pem, kafka.keystore.key and kafka.truststore.pem certs to the ${KAFKA_MOUNTED_CONF_DIR}/certs directory."
        fi
    fi
    # If SASL/SASL_SSL listeners configured, check passwords are provided
    if kafka_has_sasl_listener; then
        if is_empty_value "${KAFKA_CFG_SASL_ENABLED_MECHANISMS:-}"; then
            print_validation_error "Specified SASL protocol but no SASL mechanisms provided in KAFKA_CFG_SASL_ENABLED_MECHANISMS"
        fi
    fi
    # Check users and passwords lists are the same size
    read -r -a users <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_USERS:-}")"
    read -r -a passwords <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_PASSWORDS:-}")"
    if [[ "${#users[@]}" -ne "${#passwords[@]}" ]]; then
        print_validation_error "Specify the same number of passwords on KAFKA_CLIENT_PASSWORDS as the number of users on KAFKA_CLIENT_USERS!"
    fi
    check_multi_value "KAFKA_TLS_TYPE" "JKS PEM"
    check_multi_value "KAFKA_ZOOKEEPER_TLS_TYPE" "JKS PEM"
    check_multi_value "KAFKA_ZOOKEEPER_PROTOCOL" "PLAINTEXT SASL SSL SASL_SSL"
    check_multi_value "KAFKA_TLS_CLIENT_AUTH" "none requested required"
    [[ "$error_code" -eq 0 ]] || return "$error_code"
}

########################
# Get kafka version
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   version
#########################
kafka_get_version() {
    local -a cmd=("kafka-topics.sh" "--version")
    am_i_root && cmd=("run_as_user" "$KAFKA_DAEMON_USER" "${cmd[@]}")

    read -r -a ver_split <<< "$("${cmd[@]}")"
    echo "${ver_split[0]}"
}

#########################
# Configure JAAS for a given listener and SASL mechanisms
# Globals:
#   KAFKA_*
# Arguments:
#   $1 - Name of the listener JAAS will be configured for
#   $2 - Comma-separated list of SASL mechanisms to configure
#   $3 - Comma-separated list of usernames
#   $4 - Comma-separated list of passwords
# Returns:
#   None
#########################
kafka_configure_server_jaas() {
    local listener="${1:?missing listener name}"
    local role="${2:-}"

    if [[ "$role" = "controller" ]]; then
        local jaas_content=()
        if [[ "$KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL" = "PLAIN" ]]; then
            jaas_content=(
                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                "username=\"${KAFKA_CONTROLLER_USER}\""
                "password=\"${KAFKA_CONTROLLER_PASSWORD}\""
                "user_${KAFKA_CONTROLLER_USER}=\"${KAFKA_CONTROLLER_PASSWORD}\";"
            )
        elif [[ "$KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL" =~ SCRAM ]]; then
            jaas_content=(
                "org.apache.kafka.common.security.scram.ScramLoginModule required"
                "username=\"${KAFKA_CONTROLLER_USER}\""
                "password=\"${KAFKA_CONTROLLER_PASSWORD}\";"
            )
        fi
        listener_lower="$(echo "$listener" | tr '[:upper:]' '[:lower:]')"
        sasl_mechanism_lower="$(echo "$KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL" | tr '[:upper:]' '[:lower:]')"
        kafka_server_conf_set "listener.name.${listener_lower}.${sasl_mechanism_lower}.sasl.jaas.config" "${jaas_content[*]}"
    else
        read -r -a sasl_mechanisms_arr <<<"$(tr ',' ' ' <<<"$KAFKA_CFG_SASL_ENABLED_MECHANISMS")"
        read -r -a users <<<"$(tr ',;' ' ' <<<"$KAFKA_CLIENT_USERS")"
        read -r -a passwords <<<"$(tr ',;' ' ' <<<"$KAFKA_CLIENT_PASSWORDS")"
        # Configure JAAS for each SASL mechanism
        # ref: https://docs.confluent.io/platform/current/kafka/authentication_sasl/index.html
        for sasl_mechanism in "${sasl_mechanisms_arr[@]}"; do
            local jaas_content=()
            # For PLAIN mechanism, only the first username will be used
            if [[ "$sasl_mechanism" = "PLAIN" ]]; then
                jaas_content=("org.apache.kafka.common.security.plain.PlainLoginModule required")
                if [[ "$role" = "inter-broker" ]]; then
                    jaas_content+=(
                        "username=\"${KAFKA_INTER_BROKER_USER}\""
                        "password=\"${KAFKA_INTER_BROKER_PASSWORD}\""
                    )
                    users+=("$KAFKA_INTER_BROKER_USER")
                    passwords+=("$KAFKA_INTER_BROKER_PASSWORD")
                fi
                for ((i = 0; i < ${#users[@]}; i++)); do
                    jaas_content+=("user_${users[i]}=\"${passwords[i]}\"")
                done
                # Add semi-colon to the last element of the array
                jaas_content[${#jaas_content[@]} - 1]="${jaas_content[${#jaas_content[@]} - 1]};"
            elif [[ "$sasl_mechanism" =~ SCRAM ]]; then
                if [[ "$role" = "inter-broker" ]]; then
                    jaas_content=(
                        "org.apache.kafka.common.security.scram.ScramLoginModule required"
                        "username=\"${KAFKA_INTER_BROKER_USER}\""
                        "password=\"${KAFKA_INTER_BROKER_PASSWORD}\";"
                    )
                else
                    jaas_content=("org.apache.kafka.common.security.scram.ScramLoginModule required;")
                fi
            fi
            listener_lower="$(echo "$listener" | tr '[:upper:]' '[:lower:]')"
            sasl_mechanism_lower="$(echo "$sasl_mechanism" | tr '[:upper:]' '[:lower:]')"
            kafka_server_conf_set "listener.name.${listener_lower}.${sasl_mechanism_lower}.sasl.jaas.config" "${jaas_content[*]}"
        done
    fi
}

########################
#  Configure Zookeeper JAAS authentication
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_zookeeper_configure_jaas(){
    local jaas_content=(
        "org.apache.kafka.common.security.plain.PlainLoginModule required"
        "username=\"${KAFKA_ZOOKEEPER_USER}\""
        "password=\"${KAFKA_ZOOKEEPER_PASSWORD}\";"
    )

    kafka_server_conf_set "sasl.jaas.config" "${jaas_content[*]}"
}

########################
# Generate JAAS authentication file for local producer/consumer to use
# Globals:
#   KAFKA_*
# Arguments:
#   $1 - Authentication protocol to use for the internal listener
#   $2 - Authentication protocol to use for the client listener
# Returns:
#   None
#########################
kafka_configure_consumer_producer_jaas(){
    local jaas_content=()
    read -r -a users <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_USERS}")"
    read -r -a passwords <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_PASSWORDS}")"

    if [[ "${KAFKA_CFG_SASL_ENABLED_MECHANISMS}" =~ SCRAM ]]; then
        jaas_content=("org.apache.kafka.common.security.scram.ScramLoginModule required")
    elif [[ "${KAFKA_CFG_SASL_ENABLED_MECHANISMS}" =~ PLAIN ]]; then
        jaas_content=("org.apache.kafka.common.security.plain.PlainLoginModule required")
    else
        error "Couldn't configure a supported SASL mechanism for Kafka consumer/producer properties"
        exit 1
    fi

    jaas_content+=(
        "username=\"${users[0]}\""
        "password=\"${passwords[0]}\";"
    )

    kafka_producer_consumer_conf_set "sasl.jaas.config" "${jaas_content[*]}"
}

########################
# Create users in zookeper when using SASL/SCRAM mechanism
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_zookeeper_create_sasl_scram_users() {
    info "Creating users in Zookeeper"
    read -r -a users <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_USERS}")"
    read -r -a passwords <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_PASSWORDS}")"
    local zookeeper_connect
    zookeeper_connect=$(grep "^zookeeper.connect=" "$KAFKA_CONF_FILE" | sed -E 's/^zookeeper\.connect=(\S+)$/\1/')
    read -r -a zookeeper_hosts <<<"$(tr ',;' ' ' <<<"${zookeeper_connect}")"

    if [[ "${#zookeeper_hosts[@]}" -eq 0 ]]; then
        error "Couldn't obtain zookeeper.connect from $KAFKA_CONF_FILE"
        exit 1
    fi
    # Wait for Zookeeper to be reachable
    read -r -a aux <<<"$(tr ':' ' ' <<<"${zookeeper_hosts[0]}")"
    local host="${aux[0]:?missing host}"
    local port="${aux[1]:-2181}"
    wait-for-port --host "$host" "$port"

    # Add interbroker credentials
    if grep -Eq "^sasl.mechanism.inter.broker.protocol=SCRAM" "$KAFKA_CONF_FILE"; then
        users+=("${KAFKA_INTER_BROKER_USER}")
        passwords+=("${KAFKA_INTER_BROKER_PASSWORD}")
    fi
    for ((i = 0; i < ${#users[@]}; i++)); do
        debug "Creating user ${users[i]} in zookeeper"
        # Ref: https://docs.confluent.io/current/kafka/authentication_sasl/authentication_sasl_scram.html#sasl-scram-overview
        debug_execute kafka-configs.sh --zookeeper "$zookeeper_connect" --alter --add-config "SCRAM-SHA-256=[iterations=8192,password=${passwords[i]}],SCRAM-SHA-512=[password=${passwords[i]}]" --entity-type users --entity-name "${users[i]}"
    done
}

########################
# Configure Kafka SSL settings
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_configure_ssl() {
    # Configures both Kafka server and producers/consumers
    configure_both() {
        kafka_server_conf_set "${1:?missing key}" "${2:?missing value}"
        kafka_producer_consumer_conf_set "${1:?missing key}" "${2:?missing value}"
    }
    kafka_server_conf_set "ssl.client.auth" "${KAFKA_TLS_CLIENT_AUTH}"
    configure_both ssl.keystore.type "${KAFKA_TLS_TYPE}"
    configure_both ssl.truststore.type "${KAFKA_TLS_TYPE}"
    local -r kafka_truststore_location="${KAFKA_CERTS_DIR}/$(basename "${KAFKA_TLS_TRUSTSTORE_FILE}")"
    ! is_empty_value "${KAFKA_CERTIFICATE_PASSWORD:-}" && configure_both ssl.key.password "$KAFKA_CERTIFICATE_PASSWORD"
    if [[ "$KAFKA_TLS_TYPE" = "PEM" ]]; then
        file_to_multiline_property() {
            awk 'NR > 1{print line"\\n\\"}{line=$0;}END{print $0" "}' <"${1:?missing file}"
        }
        remove_previous_cert_value() {
            local key="${1:?missing key}"
            files=(
                "${KAFKA_CONF_FILE}"
                "${KAFKA_CONF_DIR}/producer.properties"
                "${KAFKA_CONF_DIR}/consumer.properties"
            )
            for file in "${files[@]}"; do
                if grep -q "^[#\\s]*$key\s*=.*" "$file"; then
                    # Delete all lines from the certificate beginning to its end
                    sed -i "/^[#\\s]*$key\s*=.*-----BEGIN/,/-----END/d" "$file"
                fi
            done
        }
        # We need to remove the previous cert value
        # kafka_common_conf_set uses replace_in_file, which can't match multiple lines
        remove_previous_cert_value ssl.keystore.key
        remove_previous_cert_value ssl.keystore.certificate.chain
        remove_previous_cert_value ssl.truststore.certificates
        configure_both ssl.keystore.key "$(file_to_multiline_property "${KAFKA_CERTS_DIR}/kafka.keystore.key")"
        configure_both ssl.keystore.certificate.chain "$(file_to_multiline_property "${KAFKA_CERTS_DIR}/kafka.keystore.pem")"
        configure_both ssl.truststore.certificates "$(file_to_multiline_property "${kafka_truststore_location}")"
    elif [[ "$KAFKA_TLS_TYPE" = "JKS" ]]; then
        configure_both ssl.keystore.location "$KAFKA_CERTS_DIR"/kafka.keystore.jks
        configure_both ssl.truststore.location "$kafka_truststore_location"
        ! is_empty_value "${KAFKA_CERTIFICATE_PASSWORD:-}" && configure_both ssl.keystore.password "$KAFKA_CERTIFICATE_PASSWORD"
        ! is_empty_value "${KAFKA_CERTIFICATE_PASSWORD:-}" && configure_both ssl.truststore.password "$KAFKA_CERTIFICATE_PASSWORD"
    fi
    true # Avoid the function to fail due to the check above
}

########################
# Get Zookeeper TLS settings
# Globals:
#   KAFKA_ZOOKEEPER_TLS_*
# Arguments:
#   None
# Returns:
#   String
#########################
kafka_zookeeper_configure_tls() {
    # Note that ZooKeeper does not support a key password different from the keystore password,
    # so be sure to set the key password in the keystore to be identical to the keystore password;
    # otherwise the connection attempt to Zookeeper will fail.
    local keystore_location=""
    local -r kafka_zk_truststore_location="${KAFKA_CERTS_DIR}/$(basename "${KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_FILE}")"

    if [[ "$KAFKA_ZOOKEEPER_TLS_TYPE" = "JKS" ]] && [[ -f "${KAFKA_CERTS_DIR}/zookeeper.keystore.jks" ]]; then
        keystore_location="${KAFKA_CERTS_DIR}/zookeeper.keystore.jks"
    elif [[ "$KAFKA_ZOOKEEPER_TLS_TYPE" = "PEM" ]] && [[ -f "${KAFKA_CERTS_DIR}/zookeeper.keystore.pem" ]] && [[ -f "${KAFKA_CERTS_DIR}/zookeeper.keystore.key" ]]; then
        # Concatenating private key into public certificate file
        # This is needed to load keystore from location using PEM
        keystore_location="${KAFKA_CERTS_DIR}/zookeeper.keypair.pem"
        cat "${KAFKA_CERTS_DIR}/zookeeper.keystore.pem" "${KAFKA_CERTS_DIR}/zookeeper.keystore.key" > "$keystore_location"
    fi

    kafka_server_conf_set "zookeeper.clientCnxnSocket" "org.apache.zookeeper.ClientCnxnSocketNetty"
    kafka_server_conf_set "zookeeper.ssl.client.enable" "true"
    is_boolean_yes "${KAFKA_ZOOKEEPER_TLS_VERIFY_HOSTNAME:-}" && kafka_server_conf_set "zookeeper.ssl.endpoint.identification.algorithm" "HTTPS"
    ! is_empty_value "${keystore_location:-}" && kafka_server_conf_set "zookeeper.ssl.keystore.location" "${keystore_location}"
    ! is_empty_value "${KAFKA_ZOOKEEPER_TLS_KEYSTORE_PASSWORD:-}" && kafka_server_conf_set "zookeeper.ssl.keystore.password" "${KAFKA_ZOOKEEPER_TLS_KEYSTORE_PASSWORD}"
    ! is_empty_value "${kafka_zk_truststore_location:-}" && kafka_server_conf_set "zookeeper.ssl.truststore.location" "${kafka_zk_truststore_location}"
    ! is_empty_value "${KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_PASSWORD:-}" && kafka_server_conf_set "zookeeper.ssl.truststore.password" "${KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_PASSWORD}"
    true # Avoid the function to fail due to the check above
}

########################
# Configure Kafka configuration files from environment variables
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_configure_from_environment_variables() {
    # List of special cases to apply to the variables
    local -r exception_regexps=(
        "s/sasl\.ssl/sasl_ssl/g"
        "s/sasl\.plaintext/sasl_plaintext/g"
    )
    # Map environment variables to config properties
    for var in "${!KAFKA_CFG_@}"; do
        key="$(echo "$var" | sed -e 's/^KAFKA_CFG_//g' -e 's/_/\./g' | tr '[:upper:]' '[:lower:]')"

        # Exception for the camel case in this environment variable
        [[ "$var" == "KAFKA_CFG_ZOOKEEPER_CLIENTCNXNSOCKET" ]] && key="zookeeper.clientCnxnSocket"

        # Apply exception regexps
        for regex in "${exception_regexps[@]}"; do
            key="$(echo "$key" | sed "$regex")"
        done

        value="${!var}"
        kafka_server_conf_set "$key" "$value"
    done
}

########################
# Initialize KRaft storage
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_kraft_storage_initialize() {
    local args=("--config" "$KAFKA_CONF_FILE" "--ignore-formatted")
    info "Initializing KRaft storage metadata"

    # If cluster.id found in meta.properties, use it
    if [[ -f "${KAFKA_DATA_DIR}/meta.properties" ]]; then
        KAFKA_KRAFT_CLUSTER_ID=$(grep "^cluster.id=" "${KAFKA_DATA_DIR}/meta.properties" | sed -E 's/^cluster\.id=(\S+)$/\1/')
    fi

    if is_empty_value "${KAFKA_KRAFT_CLUSTER_ID:-}"; then
        warn "KAFKA_KRAFT_CLUSTER_ID not set - If using multiple nodes then you must use the same Cluster ID for each one"
        KAFKA_KRAFT_CLUSTER_ID="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
        info "Generated Kafka cluster ID '${KAFKA_KRAFT_CLUSTER_ID}'"
    fi
    args+=("--cluster-id=$KAFKA_KRAFT_CLUSTER_ID")

    # SCRAM users are configured during the cluster bootstrapping process and can later be manually updated using kafka-config.sh
    if is_boolean_yes "${KAFKA_KRAFT_BOOTSTRAP_SCRAM_USERS:-}"; then
        info "Adding KRaft SCRAM users at storage bootstrap"
        read -r -a users <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_USERS}")"
        read -r -a passwords <<<"$(tr ',;' ' ' <<<"${KAFKA_CLIENT_PASSWORDS}")"
        # Configure SCRAM-SHA-256 if enabled
        if grep -Eq "^sasl.enabled.mechanisms=.*SCRAM-SHA-256" "$KAFKA_CONF_FILE"; then
            for ((i = 0; i < ${#users[@]}; i++)); do
                args+=("--add-scram" "SCRAM-SHA-256=[name=${users[i]},password=${passwords[i]}]")
            done
        fi
        # Configure SCRAM-SHA-512 if enabled
        if grep -Eq "^sasl.enabled.mechanisms=.*SCRAM-SHA-512" "$KAFKA_CONF_FILE"; then
            for ((i = 0; i < ${#users[@]}; i++)); do
                args+=("--add-scram" "SCRAM-SHA-512=[name=${users[i]},password=${passwords[i]}]")
            done
        fi
        # Add interbroker credentials
        if grep -Eq "^sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256" "$KAFKA_CONF_FILE"; then
            args+=("--add-scram" "SCRAM-SHA-256=[name=${KAFKA_INTER_BROKER_USER},password=${KAFKA_INTER_BROKER_PASSWORD}]")
        elif grep -Eq "^sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512" "$KAFKA_CONF_FILE"; then
            args+=("--add-scram" "SCRAM-SHA-512=[name=${KAFKA_INTER_BROKER_USER},password=${KAFKA_INTER_BROKER_PASSWORD}]")
        fi
        # Add controller credentials
        if grep -Eq "^sasl.mechanism.controller.protocol=SCRAM-SHA-256" "$KAFKA_CONF_FILE"; then
            args+=("--add-scram" "SCRAM-SHA-256=[name=${KAFKA_CONTROLLER_USER},password=${KAFKA_CONTROLLER_PASSWORD}]")
        elif grep -Eq "^sasl.mechanism.controller.protocol=SCRAM-SHA-512" "$KAFKA_CONF_FILE"; then
            args+=("--add-scram" "SCRAM-SHA-512=[name=${KAFKA_CONTROLLER_USER},password=${KAFKA_CONTROLLER_PASSWORD}]")
        fi
    fi
    info "Formatting storage directories to add metadata..."
    "${KAFKA_HOME}/bin/kafka-storage.sh" format "${args[@]}"
}

########################
# Detects inconsitences between the configuration at KAFKA_CONF_FILE and cluster-state file
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_kraft_quorum_voters_changed(){
    read -r -a quorum_voters_conf_ids <<<"$(grep "^controller.quorum.voters=" "$KAFKA_CONF_FILE" | sed "s/^controller.quorum.voters=//" | tr "," " " | sed -E "s/\@\S+//g")"
    read -r -a quorum_voters_state_ids <<< "$(grep -Eo "\{\"voterId\":[0-9]+\}" "${KAFKA_DATA_DIR}/__cluster_metadata-0/quorum-state" | grep -Eo "[0-9]+" | tr "\n" " ")"

    if [[ "${#quorum_voters_conf_ids[@]}" != "${#quorum_voters_state_ids[@]}" ]]; then
        true
    else
        read -r -a sorted_state <<< "$(echo "${quorum_voters_conf_ids[@]}" | tr ' ' '\n' | sort | tr '\n' ' ')"
        read -r -a sorted_conf <<< "$(echo "${quorum_voters_state_ids[@]}" | tr ' ' '\n' | sort | tr '\n' ' ')"
        if [[ "${sorted_state[*]}" = "${sorted_conf[*]}" ]]; then
            false
        else
            true
        fi
    fi
}

########################
# Initialize Kafka
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_initialize() {
    info "Initializing Kafka..."
    # Check for mounted configuration files
    if ! is_dir_empty "$KAFKA_MOUNTED_CONF_DIR"; then
        cp -Lr "$KAFKA_MOUNTED_CONF_DIR"/* "$KAFKA_CONF_DIR"
    fi
    # Copy truststore to cert directory
    for cert_var in KAFKA_TLS_TRUSTSTORE_FILE KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_FILE; do
        # Only copy if the file exists and it is in a different location than KAFKA_CERTS_DIR (to avoid copying to the same location)
        if [[ -f "${!cert_var}" ]] && ! [[ "${!cert_var}" =~ $KAFKA_CERTS_DIR ]]; then
            info "Copying truststore ${!cert_var} to ${KAFKA_CERTS_DIR}"
            cp -L "${!cert_var}" "$KAFKA_CERTS_DIR"
        fi
    done

    if [[ ! -f "${KAFKA_MOUNTED_CONF_DIR}/server.properties" ]]; then
        info "No injected configuration files found, creating default config files"
        # Restore original server.properties but remove Zookeeper/KRaft specific settings for compatibility with both architectures
        cp "${KAFKA_CONF_DIR}/server.properties.original" "$KAFKA_CONF_FILE"
        kafka_server_unify_conf
        # Configure Kafka settings
        kafka_server_conf_set log.dirs "$KAFKA_DATA_DIR"
        kafka_configure_from_environment_variables
        # Configure Kafka producer/consumer to set up message sizes
        ! is_empty_value "${KAFKA_CFG_MAX_REQUEST_SIZE:-}" && kafka_common_conf_set "$KAFKA_CONF_DIR/producer.properties" max.request.size "$KAFKA_CFG_MAX_REQUEST_SIZE"
        ! is_empty_value "${KAFKA_CFG_MAX_PARTITION_FETCH_BYTES:-}" && kafka_common_conf_set "$KAFKA_CONF_DIR/consumer.properties" max.partition.fetch.bytes "$KAFKA_CFG_MAX_PARTITION_FETCH_BYTES"
        # Zookeeper mode additional settings
        if ! is_empty_value "${KAFKA_CFG_ZOOKEEPER_CONNECT:-}"; then
            if [[ "$KAFKA_ZOOKEEPER_PROTOCOL" =~ SSL ]]; then
                kafka_zookeeper_configure_tls
            fi
            if [[ "$KAFKA_ZOOKEEPER_PROTOCOL" =~ SASL ]]; then
                kafka_zookeeper_configure_jaas
            fi
        fi
        # If at least one listener uses SSL or SASL_SSL, ensure SSL is configured
        if kafka_has_ssl_listener; then
            kafka_configure_ssl
        fi
        # If at least one listener uses SASL_PLAINTEXT or SASL_SSL, ensure SASL is configured
        if kafka_has_sasl_listener; then
            if [[ "$KAFKA_CFG_SASL_ENABLED_MECHANISMS" =~ SCRAM ]]; then
                if ! is_empty_value "${KAFKA_CFG_PROCESS_ROLES:-}"; then
                    if [[ "$(kafka_get_version)" =~ ^3\.2\.|^3\.3\.|^3\.4\. ]]; then
                        # NOTE: This will depend on Kafka version when support for SCRAM is added
                        warn "KRaft mode requires Kafka version 3.5 or higher for SCRAM to be supported. SCRAM SASL mechanisms will now be disabled."
                        KAFKA_CFG_SASL_ENABLED_MECHANISMS=PLAIN
                    else
                        export KAFKA_KRAFT_BOOTSTRAP_SCRAM_USERS="true"
                    fi
                fi
                if ! is_empty_value "${KAFKA_CFG_ZOOKEEPER_CONNECT:-}"; then
                    export KAFKA_ZOOKEEPER_BOOTSTRAP_SCRAM_USERS="true"
                fi
            fi
            kafka_server_conf_set sasl.enabled.mechanisms "$KAFKA_CFG_SASL_ENABLED_MECHANISMS"
        fi
        # Settings for each Kafka Listener are configured individually
        read -r -a protocol_maps <<<"$(tr ',' ' ' <<<"$KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP")"
        for protocol_map in "${protocol_maps[@]}"; do
            read -r -a map <<<"$(tr ':' ' ' <<<"$protocol_map")"
            # Obtain the listener and protocol from protocol map string, e.g. CONTROLLER:PLAINTEXT
            listener="${map[0]}"
            protocol="${map[1]}"
            listener_lower="$(echo "$listener" | tr '[:upper:]' '[:lower:]')"

            if [[ "$protocol" = "SSL" || "$protocol" = "SASL_SSL" ]]; then
                listener_upper="$(echo "$listener" | tr '[:lower:]' '[:upper:]')"
                env_name="KAFKA_TLS_${listener_upper}_CLIENT_AUTH"
                [[ -n "${!env_name:-}" ]] && kafka_server_conf_set "listener.name.${listener_lower}.ssl.client.auth" "${!env_name}"
            fi
            if [[ "$protocol" = "SASL_PLAINTEXT" || "$protocol" = "SASL_SSL" ]]; then
                local role=""
                if [[ "$listener" = "${KAFKA_CFG_INTER_BROKER_LISTENER_NAME:-INTERNAL}" ]]; then
                    kafka_server_conf_set sasl.mechanism.inter.broker.protocol "$KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL"
                    role="inter-broker"
                elif [[ "${KAFKA_CFG_CONTROLLER_LISTENER_NAMES:-CONTROLLER}" =~ $listener ]]; then
                    kafka_server_conf_set sasl.mechanism.controller.protocol "$KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL"
                    kafka_server_conf_set "listener.name.${listener_lower}.sasl.enabled.mechanisms" "$KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL"
                    role="controller"
                fi
                # If KAFKA_CLIENT_LISTENER_NAME is found in the listeners list, configure the producer/consumer accordingly
                if [[ "$listener" = "${KAFKA_CLIENT_LISTENER_NAME:-CLIENT}" ]]; then
                    kafka_configure_consumer_producer_jaas
                    kafka_producer_consumer_conf_set security.protocol "$protocol"
                    kafka_producer_consumer_conf_set sasl.mechanism "${KAFKA_CLIENT_SASL_MECHANISM:-$(kafka_client_sasl_mechanism)}"
                fi
                # Configure inline listener jaas configuration, omitted if mounted JAAS conf file detected
                if [[ ! -f "${KAFKA_CONF_DIR}/kafka_jaas.conf" ]]; then
                    kafka_configure_server_jaas "$listener_lower" "${role:-}"
                fi
            fi
        done
        # Configure Kafka using environment variables
        # This is executed at the end, to allow users to override properties set by the initialization logic
        kafka_configure_from_environment_variables
    else
        info "Detected mounted server.properties file at ${KAFKA_MOUNTED_CONF_DIR}/server.properties. Skipping configuration based on env variables"
    fi
    true
}

########################
# Returns the most secure SASL mechanism available for Kafka clients
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
########################
kafka_client_sasl_mechanism() {
    local sasl_mechanism=""

    if [[ "$KAFKA_CFG_SASL_ENABLED_MECHANISMS" =~ SCRAM-SHA-512 ]]; then
        sasl_mechanism="SCRAM-SHA-512"
    elif [[ "$KAFKA_CFG_SASL_ENABLED_MECHANISMS" =~ SCRAM-SHA-256 ]]; then
        sasl_mechanism="SCRAM-SHA-256"
    elif [[ "$KAFKA_CFG_SASL_ENABLED_MECHANISMS" =~ PLAIN ]]; then
        sasl_mechanism="PLAIN"
    fi
    echo "$sasl_mechanism"
}

########################
# Removes default settings referencing Zookeeper mode or KRaft mode
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
########################
kafka_server_unify_conf() {
    local -r remove_regexps=(
        #Zookeeper
        "s/^zookeeper\./#zookeeper./g"
        "s/^group\.initial/#group.initial/g"
        "s/^broker\./#broker./g"
        "s/^node\./#node./g"
        "s/^process\./#process./g"
        "s/^listeners=/#listeners=/g"
        "s/^listener\./#listener./g"
        "s/^controller\./#controller./g"
        "s/^inter\.broker/#inter.broker/g"
        "s/^advertised\.listeners/#advertised.listeners/g"
    )

    # Map environment variables to config properties
    for regex in "${remove_regexps[@]}"; do
        sed -i "${regex}" "$KAFKA_CONF_FILE"
    done
}

########################
# Dinamically set node.id/broker.id/controller.quorum.voters if their alternative environment variable _COMMAND is set
# Globals:
#   KAFKA_*_COMMAND
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_dynamic_environment_variables() {
    # KRaft mode
    if ! is_empty_value "${KAFKA_NODE_ID_COMMAND:-}"; then
        KAFKA_CFG_NODE_ID="$(eval "${KAFKA_NODE_ID_COMMAND}")"
        export KAFKA_CFG_NODE_ID
    fi
    if ! is_empty_value "${KAFKA_CONTROLLER_QUORUM_VOTERS_COMMAND:-}"; then
        KAFKA_CFG_CONTROLLER_QUORUM_VOTERS="$(eval "${KAFKA_CONTROLLER_QUORUM_VOTERS_COMMAND}")"
        export KAFKA_CFG_CONTROLLER_QUORUM_VOTERS
    fi
    # Zookeeper mode
    # DEPRECATED - BROKER_ID_COMMAND has been deprecated, please use KAFKA_BROKER_ID_COMMAND instead
    if ! is_empty_value "${KAFKA_BROKER_ID_COMMAND:-}"; then
        KAFKA_CFG_BROKER_ID="$(eval "${KAFKA_BROKER_ID_COMMAND}")"
        export KAFKA_CFG_BROKER_ID
    elif ! is_empty_value "${BROKER_ID_COMMAND:-}"; then
        KAFKA_CFG_BROKER_ID="$(eval "${BROKER_ID_COMMAND}")"
        export KAFKA_CFG_BROKER_ID
    fi
}

########################
# Run custom initialization scripts
# Globals:
#   KAFKA_*
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_custom_init_scripts() {
    if [[ -n $(find "${KAFKA_INITSCRIPTS_DIR}/" -type f -regex ".*\.\(sh\)") ]] && [[ ! -f "${KAFKA_VOLUME_DIR}/.user_scripts_initialized" ]]; then
        info "Loading user's custom files from $KAFKA_INITSCRIPTS_DIR"
        for f in /docker-entrypoint-initdb.d/*; do
            debug "Executing $f"
            case "$f" in
            *.sh)
                if [[ -x "$f" ]]; then
                    if ! "$f"; then
                        error "Failed executing $f"
                        return 1
                    fi
                else
                    warn "Sourcing $f as it is not executable by the current user, any error may cause initialization to fail"
                    . "$f"
                fi
                ;;
            *)
                warn "Skipping $f, supported formats are: .sh"
                ;;
            esac
        done
        touch "$KAFKA_VOLUME_DIR"/.user_scripts_initialized
    fi
}

########################
# Check if Kafka is running
# Globals:
#   KAFKA_PID_FILE
# Arguments:
#   None
# Returns:
#   Whether Kafka is running
########################
is_kafka_running() {
    local pid
    pid="$(get_pid_from_file "$KAFKA_PID_FILE")"
    if [[ -n "$pid" ]]; then
        is_service_running "$pid"
    else
        false
    fi
}

########################
# Check if Kafka is running
# Globals:
#   KAFKA_PID_FILE
# Arguments:
#   None
# Returns:
#   Whether Kafka is not running
########################
is_kafka_not_running() {
    ! is_kafka_running
}

########################
# Stop Kafka
# Globals:
#   KAFKA_PID_FILE
# Arguments:
#   None
# Returns:
#   None
#########################
kafka_stop() {
    ! is_kafka_running && return
    stop_service_using_pid "$KAFKA_PID_FILE" TERM
}
