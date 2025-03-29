#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source ~/.bashrc

script_path="${0}"

# The absolute path to the directory which this script is in.
start_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

run_info_file="${start_dir}/run.info"

# The absolute path to the root Kafka directory
kafka_dir="$( cd "${start_dir}/../kafka" && pwd )"

data_path="/data/kafka"

# Exit with an error message.
die() {
  echo "$@"
  exit 1
}

echo_and_do() {
    local cmd="${@}"
    echo "${cmd}"
    ${cmd}
}

# Run a command and die if it fails.
#
# Optional flags:
# -v: print the command before running it.
# -o: display the command output.
# $@: The command to run.
must_do() {
  local verbose=0
  local output="/dev/null"
  while true; do
    case ${1} in
    -v)
      verbose=1
      shift
      ;;
    -o)
      output="/dev/stdout"
      shift
      ;;
    *) break ;;
    esac
  done
  local cmd="$*"
  [[ "${verbose}" -eq 1 ]] && echo "${cmd}"
  eval "${cmd}" >${output} || die "${1} failed"
}

# Display a usage message on the terminal and exit.
#
# $1: The exit status to use
usage() {
  local exit_status="${1}"
  cat <<EOF
start: a tool for starting 'AutoMQ for Apache Kafka on S3'.

Usage: ${script_path} [command] [options]

help|-h|--help
    Display this help message
up [--process.roles ROLE] [--node.id NODE_ID] [--controller.quorum.voters VOTERS]
   [--s3.region REGION] [--s3.bucket BUCKET] [--s3.endpoint ENDPOINT]
   [--s3.access.key ACCESS_KEY] [--s3.secret.key SECRET_KEY]
    start node.
EOF
  exit "${exit_status}"
}

# Check for the presence of certain commands.
#
# $@: The commands to check for.  This function will die if any of these commands are not found by
#       the 'which' command.
require_commands() {
  local cmds=("$@")
  for cmd in "${cmds[@]}"; do
    which -- "${cmd}" &>/dev/null || die "You must install ${cmd} to run this script."
  done
}

# Set a global variable to a value.
#
# $1: The variable name to set.  This function will die if the variable already has a value.  The
#     variable will be made readonly to prevent any future modifications.
# $2: The value to set the variable to.  This function will die if the value is empty or starts
#     with a dash.
# $3: A human-readable description of the variable.
set_once() {
    local key="${1}"
    local value="${2}"
    local what="${3}"
    [[ -n "${!key}" ]] && die "Error: more than one value specified for ${what}."
    verify_command_line_argument "${value}" "${what}"
    # It would be better to use declare -g, but older bash versions don't support it.
    export "${key}"="${value}"
}

# Verify that a command-line argument is present and does not start with a slash.
#
# $1: The command-line argument to verify.
# $2: A human-readable description of the variable.
verify_command_line_argument() {
    local value="${1}"
    local what="${2}"
    [[ -n "${value}" ]] || die "Error: no value specified for ${what}"
    [[ ${value} == -* ]] && die "Error: invalid value ${value} specified for ${what}"
}

setup_value() {
  key=$1
  value=$2
  file=$3
  # replace special characters
  value=${value//&/\\&}
  value=${value//#//\\#/}
  echo "setup_value: key=${key}, value=${value}, file=${file}"
  sed -i "s|^${key}=.*$|${key}=${value}|" "${file}"
}

add_or_setup_value() {
  key=$1
  value=$2
  file=$3
  if grep -q "^${key}" "${file}"; then
    setup_value "${key}" "${value}" "${file}"
  else
    echo "${key}=${value}" | tee -a "${file}" >/dev/null
  fi
}

auto_balancer_setting_for_all() {
    file_name=$1
    add_or_setup_value "autobalancer.topic" "AutoBalancerMetricsReporterTopic" "${file_name}"
    add_or_setup_value "autobalancer.topic.num.partitions" "1" "${file_name}"
}

auto_balancer_setting_for_controller_only() {
    file_name=$1
    add_or_setup_value "autobalancer.controller.enable" "true" "${file_name}"
    add_or_setup_value "autobalancer.controller.exclude.topics" "__consumer_offsets" "${file_name}"
}

auto_balancer_setting_for_broker_only() {
    file_name=$1
    add_or_setup_value "metric.reporters" "kafka.autobalancer.metricsreporter.AutoBalancerMetricsReporter" "${file_name}"
}

turn_on_auto_balancer() {
    role=$1
    file_name=$2
    auto_balancer_setting_for_all "${file_name}"
    if [[ "${role}" == "broker" ]]; then
        auto_balancer_setting_for_broker_only "${file_name}"
    elif [[ "${role}" == "controller" ]]; then
        auto_balancer_setting_for_controller_only "${file_name}"
    elif [[ "${role}" == "server" ]]; then
        auto_balancer_setting_for_controller_only "${file_name}"
        auto_balancer_setting_for_broker_only "${file_name}"
    fi
}


# monitor and change advertised ip for kafka
kafka_monitor_ip() {
    process_role=$(grep "role" "${run_info_file}" | awk -F= '{print $2}')
    [[ -n "${process_role}" ]] || die "kafka_down: failed to get node role"

    # get private ip first
    local_private_ip=$(hostname -I | awk '{print $1}')
    if [[ "x${local_private_ip}" == "x" || "x${local_private_ip}" == "x127.0.0.1" ]]; then
        die "kafka_start_up: failed to find the local private IP address."
    fi
    advertised_ip="${local_private_ip}"

    # change ip settings for this node
    if [[ "${process_role}" == "server" ]]; then
        setup_value "listeners" "PLAINTEXT://${local_private_ip}:9092,CONTROLLER://${local_private_ip}:9093" "${kafka_dir}/config/kraft/${process_role}.properties"
        setup_value "advertised.listeners" "PLAINTEXT://${advertised_ip}:9092" "${kafka_dir}/config/kraft/${process_role}.properties"
    elif [[ "${process_role}" == "broker" ]]; then
        setup_value "listeners" "PLAINTEXT://${local_private_ip}:9092" "${kafka_dir}/config/kraft/${process_role}.properties"
        setup_value "advertised.listeners" "PLAINTEXT://${advertised_ip}:9092" "${kafka_dir}/config/kraft/${process_role}.properties"
    elif [[ "${process_role}" == "controller" ]]; then
        setup_value "listeners" "CONTROLLER://${local_private_ip}:9093" "${kafka_dir}/config/kraft/${process_role}.properties"
        setup_value "advertised.listeners" "CONTROLLER://${local_private_ip}:9093" "${kafka_dir}/config/kraft/${process_role}.properties"
    else
        die "kafka_monitor_ip: unknown process role ${process_role}"
    fi
}

configure_from_environment_variables() {
    file_name=$1
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
        add_or_setup_value "${key}" "${value}" "${file_name}"
    done
}

kafka_up() {
  echo "kafka_up: start"

  while [[ $# -ge 1 ]]; do
      case "${1}" in
          --process.roles) set_once process_role "${2}" "role of this node"; shift 2;;
          --node.id) set_once node_id "${2}" "id of this node"; shift 2;;
          --controller.quorum.voters) set_once quorum_voters "${2}" "controller quorum voters"; shift 2;;
          --s3.region) set_once s3_region "${2}" "regions of s3"; shift 2;;
          --s3.bucket) set_once s3_bucket "${2}" "bucket name of s3"; shift 2;;
          --cluster.id) set_once cluster_id "${2}" "kafka cluster id"; shift 2;;
          --s3.access.key) set_once s3_access_key "${2}" "s3 access key"; shift 2;;
          --s3.secret.key) set_once s3_secret_key "${2}" "s3 secret key"; shift 2;;
          --s3.endpoint) set_once s3_endpoint "${2}" "s3 endpoint"; shift 2;;
      esac
  done

  pid=$(jcmd | grep -e kafka.Kafka | awk '{print $1}')
  if [[ -n "${pid}" ]]; then
      echo "kafka_up: kafka is already running, pid=${pid}"
      exit 0
  fi

  [[ -n "${node_id}" ]] || die "node_id is empty"
  [[ -n "${process_role}" ]] || die "process_role is empty"
  [[ -n "${quorum_voters}" ]] || die "quorum_voters is empty"
  [[ -n "${s3_region}" ]] || s3_region="${AWS_DEFAULT_REGION}"
  [[ -n "${s3_bucket}" ]] || die "s3_bucket is empty"
  [[ -n "${s3_access_key}" ]] || s3_access_key="${KAFKA_S3_ACCESS_KEY}"
  [[ -n "${s3_secret_key}" ]] || s3_secret_key="${KAFKA_S3_SECRET_KEY}"
  [[ -n "${s3_endpoint}" ]] || die "s3_endpoint is empty"
  [[ -n "${cluster_id}" ]] || cluster_id="rZdE0DjZSrqy96PXrMUZVw"

  quorum_bootstrap_servers=$(echo "${quorum_voters}" | sed 's/[0-9]*@//g')

  for role in "broker" "controller" "server"; do
      setup_value "node.id" "${node_id}" "${kafka_dir}/config/kraft/${role}.properties"
      add_or_setup_value "controller.quorum.voters" "${quorum_voters}" "${kafka_dir}/config/kraft/${role}.properties"
      setup_value "controller.quorum.bootstrap.servers" "${quorum_bootstrap_servers}" "${kafka_dir}/config/kraft/${role}.properties"
      setup_value "s3.data.buckets" "0@s3://${s3_bucket}?region=${s3_region}&endpoint=${s3_endpoint}&authType=static" "${kafka_dir}/config/kraft/${role}.properties"
      setup_value "s3.ops.buckets" "0@s3://${s3_bucket}?region=${s3_region}&endpoint=${s3_endpoint}&authType=static" "${kafka_dir}/config/kraft/${role}.properties"
      setup_value "log.dirs" "${data_path}/kraft-${role}-logs" "${kafka_dir}/config/kraft/${role}.properties"
      setup_value "s3.wal.path" "0@file://${data_path}/wal?capacity=2147483648" "${kafka_dir}/config/kraft/${role}.properties"
      # turn on auto_balancer
      turn_on_auto_balancer "${role}" "${kafka_dir}/config/kraft/${role}.properties"
  done

  if [[ -n "${KAFKA_HEAP_OPTS}" ]]; then
      kafka_heap_opts="${KAFKA_HEAP_OPTS}"
  elif [[ "${process_role}" == "broker" || "${process_role}" == "server" ]]; then
      kafka_heap_opts="-Xms1g -Xmx1g -XX:MetaspaceSize=96m -XX:MaxDirectMemorySize=1G"
  elif [[ "${process_role}" == "controller" ]]; then
      kafka_heap_opts="-Xms1g -Xmx1g -XX:MetaspaceSize=96m"
  else
      die "kafka_start_up: unknown process role ${process_role}"
  fi

  # add this node's info to run.info
  setup_value "node.id" "${node_id}" "${run_info_file}"
  setup_value "role" "${process_role}" "${run_info_file}"
  setup_value "kafka.base.path" "${kafka_dir}" "${run_info_file}"
  setup_value "kafka.data.path" "${data_path}" "${run_info_file}"

  # change ip settings here
  kafka_monitor_ip
  echo "kafka_up: ip settings changed"

  # override settings from env
  configure_from_environment_variables "${kafka_dir}/config/kraft/${process_role}.properties"

  # Disable the default console logger in favour of KafkaAppender (which provides the exact output)
  echo "log4j.appender.stdout.Threshold=OFF" >> "${kafka_dir}/config/log4j.properties"

  # format the data path
  must_do -v "${kafka_dir}/bin/kafka-storage.sh format -g -t ${cluster_id} -c ${kafka_dir}/config/kraft/${process_role}.properties"

  exec "${kafka_dir}/bin/kafka-server-start.sh" "${kafka_dir}/config/kraft/${process_role}.properties"
}

# Parse command-line arguments
[[ $# -lt 1 ]] && usage 0
# Display the help text if -h or --help appears in the command line
for arg in "${@}"; do
  case "${arg}" in
  -h | --help) usage 0 ;;
  --) break ;;
  *) ;;
  esac
done
action="${1}"
shift
case "${action}" in
help) usage 0 ;;

up)
  kafka_"${action}" "${@}"
  exit 0
  ;;

*)
  echo "Unknown command '${action}'.  Type '${script_path} --help' for usage information."
  exit 1
  ;;
esac
