#!/bin/bash
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0
#
# Library for managing services

# shellcheck disable=SC1091

# Load Generic Libraries
. /opt/bitnami/scripts/libvalidations.sh
. /opt/bitnami/scripts/liblog.sh

# Functions

########################
# Read the provided pid file and returns a PID
# Arguments:
#   $1 - Pid file
# Returns:
#   PID
#########################
get_pid_from_file() {
    local pid_file="${1:?pid file is missing}"

    if [[ -f "$pid_file" ]]; then
        if [[ -n "$(< "$pid_file")" ]] && [[ "$(< "$pid_file")" -gt 0 ]]; then
            echo "$(< "$pid_file")"
        fi
    fi
}

########################
# Check if a provided PID corresponds to a running service
# Arguments:
#   $1 - PID
# Returns:
#   Boolean
#########################
is_service_running() {
    local pid="${1:?pid is missing}"

    kill -0 "$pid" 2>/dev/null
}

########################
# Stop a service by sending a termination signal to its pid
# Arguments:
#   $1 - Pid file
#   $2 - Signal number (optional)
# Returns:
#   None
#########################
stop_service_using_pid() {
    local pid_file="${1:?pid file is missing}"
    local signal="${2:-}"
    local pid

    pid="$(get_pid_from_file "$pid_file")"
    [[ -z "$pid" ]] || ! is_service_running "$pid" && return

    if [[ -n "$signal" ]]; then
        kill "-${signal}" "$pid"
    else
        kill "$pid"
    fi

    local counter=10
    while [[ "$counter" -ne 0 ]] && is_service_running "$pid"; do
        sleep 1
        counter=$((counter - 1))
    done
}

########################
# Start cron daemon
# Arguments:
#   None
# Returns:
#   true if started correctly, false otherwise
#########################
cron_start() {
    if [[ -x "/usr/sbin/cron" ]]; then
        /usr/sbin/cron
    elif [[ -x "/usr/sbin/crond" ]]; then
        /usr/sbin/crond
    else
        false
    fi
}

########################
# Generate a cron configuration file for a given service
# Arguments:
#   $1 - Service name
#   $2 - Command
# Flags:
#   --run-as - User to run as (default: root)
#   --schedule - Cron schedule configuration (default: * * * * *)
# Returns:
#   None
#########################
generate_cron_conf() {
    local service_name="${1:?service name is missing}"
    local cmd="${2:?command is missing}"
    local run_as="root"
    local schedule="* * * * *"
    local clean="true"

    # Parse optional CLI flags
    shift 2
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --run-as)
                shift
                run_as="$1"
                ;;
            --schedule)
                shift
                schedule="$1"
                ;;
            --no-clean)
                clean="false"
                ;;
            *)
                echo "Invalid command line flag ${1}" >&2
                return 1
                ;;
        esac
        shift
    done

    mkdir -p /etc/cron.d
    if "$clean"; then
        cat > "/etc/cron.d/${service_name}" <<EOF
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

${schedule} ${run_as} ${cmd}
EOF
    else
        echo "${schedule} ${run_as} ${cmd}" >> /etc/cron.d/"$service_name"
    fi
}

########################
# Remove a cron configuration file for a given service
# Arguments:
#   $1 - Service name
# Returns:
#   None
#########################
remove_cron_conf() {
    local service_name="${1:?service name is missing}"
    local cron_conf_dir="/etc/monit/conf.d"
    rm -f "${cron_conf_dir}/${service_name}"
}

########################
# Generate a monit configuration file for a given service
# Arguments:
#   $1 - Service name
#   $2 - Pid file
#   $3 - Start command
#   $4 - Stop command
# Flags:
#   --disable - Whether to disable the monit configuration
# Returns:
#   None
#########################
generate_monit_conf() {
    local service_name="${1:?service name is missing}"
    local pid_file="${2:?pid file is missing}"
    local start_command="${3:?start command is missing}"
    local stop_command="${4:?stop command is missing}"
    local monit_conf_dir="/etc/monit/conf.d"
    local disabled="no"

    # Parse optional CLI flags
    shift 4
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --disable)
                disabled="yes"
                ;;
            *)
                echo "Invalid command line flag ${1}" >&2
                return 1
                ;;
        esac
        shift
    done

    is_boolean_yes "$disabled" && conf_suffix=".disabled"
    mkdir -p "$monit_conf_dir"
    cat > "${monit_conf_dir}/${service_name}.conf${conf_suffix:-}" <<EOF
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

check process ${service_name}
  with pidfile "${pid_file}"
  start program = "${start_command}" with timeout 90 seconds
  stop program = "${stop_command}" with timeout 90 seconds
EOF
}

########################
# Remove a monit configuration file for a given service
# Arguments:
#   $1 - Service name
# Returns:
#   None
#########################
remove_monit_conf() {
    local service_name="${1:?service name is missing}"
    local monit_conf_dir="/etc/monit/conf.d"
    rm -f "${monit_conf_dir}/${service_name}.conf"
}

########################
# Generate a logrotate configuration file
# Arguments:
#   $1 - Service name
#   $2 - Log files pattern
# Flags:
#   --period - Period
#   --rotations - Number of rotations to store
#   --extra - Extra options (Optional)
# Returns:
#   None
#########################
generate_logrotate_conf() {
    local service_name="${1:?service name is missing}"
    local log_path="${2:?log path is missing}"
    local period="weekly"
    local rotations="150"
    local extra=""
    local logrotate_conf_dir="/etc/logrotate.d"
    local var_name
    # Parse optional CLI flags
    shift 2
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --period|--rotations|--extra)
                var_name="$(echo "$1" | sed -e "s/^--//" -e "s/-/_/g")"
                shift
                declare "$var_name"="${1:?"$var_name" is missing}"
                ;;
            *)
                echo "Invalid command line flag ${1}" >&2
                return 1
                ;;
        esac
        shift
    done

    mkdir -p "$logrotate_conf_dir"
    cat <<EOF | sed '/^\s*$/d' > "${logrotate_conf_dir}/${service_name}"
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

${log_path} {
  ${period}
  rotate ${rotations}
  dateext
  compress
  copytruncate
  missingok
$(indent "$extra" 2)
}
EOF
}

########################
# Remove a logrotate configuration file
# Arguments:
#   $1 - Service name
# Returns:
#   None
#########################
remove_logrotate_conf() {
    local service_name="${1:?service name is missing}"
    local logrotate_conf_dir="/etc/logrotate.d"
    rm -f "${logrotate_conf_dir}/${service_name}"
}

########################
# Generate a Systemd configuration file
# Arguments:
#   $1 - Service name
# Flags:
#   --custom-service-content - Custom content to add to the [service] block
#   --environment - Environment variable to define (multiple --environment options may be passed)
#   --environment-file - Text file with environment variables (multiple --environment-file options may be passed)
#   --exec-start - Start command (required)
#   --exec-start-pre - Pre-start command (optional)
#   --exec-start-post - Post-start command (optional)
#   --exec-stop - Stop command (optional)
#   --exec-reload - Reload command (optional)
#   --group - System group to start the service with
#   --name - Service full name (e.g. Apache HTTP Server, defaults to $1)
#   --restart - When to restart the Systemd service after being stopped (defaults to always)
#   --pid-file - Service PID file
#   --standard-output - File where to print stdout output
#   --standard-error - File where to print stderr output
#   --success-exit-status - Exit code that indicates a successful shutdown
#   --type - Systemd unit type (defaults to forking)
#   --user - System user to start the service with
#   --working-directory - Working directory at which to start the service
# Returns:
#   None
#########################
generate_systemd_conf() {
    local -r service_name="${1:?service name is missing}"
    local -r systemd_units_dir="/etc/systemd/system"
    local -r service_file="${systemd_units_dir}/bitnami.${service_name}.service"
    # Default values
    local name="$service_name"
    local type="forking"
    local user=""
    local group=""
    local environment=""
    local environment_file=""
    local exec_start=""
    local exec_start_pre=""
    local exec_start_post=""
    local exec_stop=""
    local exec_reload=""
    local restart="always"
    local pid_file=""
    local standard_output="journal"
    local standard_error=""
    local limits_content=""
    local success_exit_status=""
    local custom_service_content=""
    local working_directory=""
    # Parse CLI flags
    shift
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --name \
            | --type \
            | --user \
            | --group \
            | --exec-start \
            | --exec-stop \
            | --exec-reload \
            | --restart \
            | --pid-file \
            | --standard-output \
            | --standard-error \
            | --success-exit-status \
            | --custom-service-content \
            | --working-directory \
            )
                var_name="$(echo "$1" | sed -e "s/^--//" -e "s/-/_/g")"
                shift
                declare "$var_name"="${1:?"${var_name} value is missing"}"
                ;;
            --limit-*)
                [[ -n "$limits_content" ]] && limits_content+=$'\n'
                var_name="${1//--limit-}"
                shift
                limits_content+="Limit${var_name^^}=${1:?"--limit-${var_name} value is missing"}"
                ;;
            --exec-start-pre)
                shift
                [[ -n "$exec_start_pre" ]] && exec_start_pre+=$'\n'
                exec_start_pre+="ExecStartPre=${1:?"--exec-start-pre value is missing"}"
                ;;
            --exec-start-post)
                shift
                [[ -n "$exec_start_post" ]] && exec_start_post+=$'\n'
                exec_start_post+="ExecStartPost=${1:?"--exec-start-post value is missing"}"
                ;;
            --environment)
                shift
                # It is possible to add multiple environment lines
                [[ -n "$environment" ]] && environment+=$'\n'
                environment+="Environment=${1:?"--environment value is missing"}"
                ;;
            --environment-file)
                shift
                # It is possible to add multiple environment-file lines
                [[ -n "$environment_file" ]] && environment_file+=$'\n'
                environment_file+="EnvironmentFile=${1:?"--environment-file value is missing"}"
                ;;
            *)
                echo "Invalid command line flag ${1}" >&2
                return 1
                ;;
        esac
        shift
    done
    # Validate inputs
    local error="no"
    if [[ -z "$exec_start" ]]; then
        error "The --exec-start option is required"
        error="yes"
    fi
    if [[ "$error" != "no" ]]; then
        return 1
    fi
    # Generate the Systemd unit
    cat > "$service_file" <<EOF
# Copyright Broadcom, Inc. All Rights Reserved.
# SPDX-License-Identifier: APACHE-2.0

[Unit]
Description=Bitnami service for ${name}
# Starting/stopping the main bitnami service should cause the same effect for this service
PartOf=bitnami.service

[Service]
Type=${type}
EOF
    if [[ -n "$working_directory" ]]; then
        cat >> "$service_file" <<< "WorkingDirectory=${working_directory}"
    fi
    if [[ -n "$exec_start_pre" ]]; then
        # This variable may contain multiple ExecStartPre= directives
        cat >> "$service_file" <<< "$exec_start_pre"
    fi
    if [[ -n "$exec_start" ]]; then
        cat >> "$service_file" <<< "ExecStart=${exec_start}"
    fi
    if [[ -n "$exec_start_post" ]]; then
        # This variable may contain multiple ExecStartPost= directives
        cat >> "$service_file" <<< "$exec_start_post"
    fi
    # Optional stop and reload commands
    if [[ -n "$exec_stop" ]]; then
        cat >> "$service_file" <<< "ExecStop=${exec_stop}"
    fi
    if [[ -n "$exec_reload" ]]; then
        cat >> "$service_file" <<< "ExecReload=${exec_reload}"
    fi
    # User and group
    if [[ -n "$user" ]]; then
        cat >> "$service_file" <<< "User=${user}"
    fi
    if [[ -n "$group" ]]; then
        cat >> "$service_file" <<< "Group=${group}"
    fi
    # PID file allows to determine if the main process is running properly (for Restart=always)
    if [[ -n "$pid_file" ]]; then
        cat >> "$service_file" <<< "PIDFile=${pid_file}"
    fi
    if [[ -n "$restart" ]]; then
        cat >> "$service_file" <<< "Restart=${restart}"
    fi
    # Environment flags
    if [[ -n "$environment" ]]; then
        # This variable may contain multiple Environment= directives
        cat >> "$service_file" <<< "$environment"
    fi
    if [[ -n "$environment_file" ]]; then
        # This variable may contain multiple EnvironmentFile= directives
        cat >> "$service_file" <<< "$environment_file"
    fi
    # Logging
    if [[ -n "$standard_output" ]]; then
        cat >> "$service_file" <<< "StandardOutput=${standard_output}"
    fi
    if [[ -n "$standard_error" ]]; then
        cat >> "$service_file" <<< "StandardError=${standard_error}"
    fi
    if [[ -n "$custom_service_content" ]]; then
        # This variable may contain multiple miscellaneous directives
        cat >> "$service_file" <<< "$custom_service_content"
    fi
    if [[ -n "$success_exit_status" ]]; then
        cat >> "$service_file" <<EOF
# When the process receives a SIGTERM signal, it exits with code ${success_exit_status}
SuccessExitStatus=${success_exit_status}
EOF
    fi
    cat >> "$service_file" <<EOF
# Optimizations
TimeoutStartSec=2min
TimeoutStopSec=30s
IgnoreSIGPIPE=no
KillMode=mixed
EOF
    if [[ -n "$limits_content" ]]; then
        cat >> "$service_file" <<EOF
# Limits
${limits_content}
EOF
    fi
    cat >> "$service_file" <<EOF

[Install]
# Enabling/disabling the main bitnami service should cause the same effect for this service
WantedBy=bitnami.service
EOF
}
