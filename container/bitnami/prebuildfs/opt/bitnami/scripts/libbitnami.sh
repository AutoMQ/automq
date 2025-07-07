#!/bin/bash
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0
#
# Bitnami custom library

# shellcheck disable=SC1091

# Load Generic Libraries
. /opt/bitnami/scripts/liblog.sh

# Constants
BOLD='\033[1m'

# Functions

########################
# Print the welcome page
# Globals:
#   DISABLE_WELCOME_MESSAGE
#   BITNAMI_APP_NAME
# Arguments:
#   None
# Returns:
#   None
#########################
print_welcome_page() {
    if [[ -z "${DISABLE_WELCOME_MESSAGE:-}" ]]; then
        if [[ -n "$BITNAMI_APP_NAME" ]]; then
            print_image_welcome_page
        fi
    fi
}

########################
# Print the welcome page for a Bitnami Docker image
# Globals:
#   BITNAMI_APP_NAME
# Arguments:
#   None
# Returns:
#   None
#########################
print_image_welcome_page() {
    local docs_url="https://www.automq.com/docs/automq/deployment/deploy-multi-nodes-cluster-on-kubernetes"

    info ""
    info "${BOLD}Welcome to the AutoMQ for Apache Kafka on Bitnami Container${RESET}"
    info "${BOLD}This image is compatible with Bitnami's container standards.${RESET}"
    info "Refer to the documentation for complete configuration and Kubernetes deployment guidelines:"
    info "${BOLD}${docs_url}${RESET}"
    info ""
}

