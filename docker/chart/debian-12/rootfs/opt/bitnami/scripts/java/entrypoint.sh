#!/bin/bash
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

# shellcheck disable=SC1091

set -o errexit
set -o nounset
set -o pipefail
# set -o xtrace # Uncomment this line for debugging purposes

# Load libraries
. /opt/bitnami/scripts/libbitnami.sh
. /opt/bitnami/scripts/liblog.sh

if [[ "$OS_FLAVOUR" =~ photon && "$APP_VERSION" =~ ^1.8 ]]; then
  # Option --module-path is not supported by JAVA 1.8 since modules were added in version 1.9
  unset JAVA_TOOL_OPTIONS
fi

print_welcome_page

echo ""
exec "$@"
