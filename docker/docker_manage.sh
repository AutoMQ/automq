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

script_path="${0}"

# The absolute path to the directory which this script is in.
start_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The absolute path to the root Kafka directory
kafka_dir="$(cd "${start_dir}/.." && pwd)"

kafka_version="3.4.0"
skip_pkg="0"

docker_kafka_base_dir="/opt/automq/kafka"
docker_kafka_data_dir="/data/kafka"


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
docker_manage: a tool for managing docker images of 'AutoMQ for Apache Kafka on S3'.

Usage: ${script_path} [command] [options]

help|-h|--help
    Display this help message
build
    build docker images.
push
    push docker images to docker hub.
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
        which -- "${cmd}" &> /dev/null || die "You must install ${cmd} to run this script."
    done
}

prepare() {
  require_commands java docker
  if [[ "${skip_pkg}" -eq "0" ]]; then
    must_do cd "${kafka_dir}"
    must_do -v ./gradlew clean releaseTarGz
  fi
  must_do -v cp "${kafka_dir}/core/build/distributions/kafka_2.13-${kafka_version}.tgz" ${start_dir}/
  must_do cd "${start_dir}"
}

docker_push() {
    while [[ $# -ge 1 ]]; do
        case "${1}" in
            --version) kafka_version="${2}"; shift 2;;
            --skip-pkg) skip_pkg="1"; shift 1;;
        esac
    done
    prepare
    echo_and_do docker buildx build --platform linux/amd64,linux/arm64 \
          -t "automqinc/kafka:${kafka_version}" \
          . --push
}

docker_build() {
    while [[ $# -ge 1 ]]; do
        case "${1}" in
            --version) kafka_version="${2}"; shift 2;;
            --skip-pkg) skip_pkg="1"; shift 1;;
        esac
    done
    prepare
    echo_and_do docker build -t "automqinc/kafka:${kafka_version}" .
#        --build-arg "general_mirror_url=mirrors.ustc.edu.cn" .

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

push|build)
  docker_"${action}" "${@}"
  exit 0
  ;;

*)
  echo "Unknown command '${action}'.  Type '${script_path} --help' for usage information."
  exit 1
  ;;
esac
