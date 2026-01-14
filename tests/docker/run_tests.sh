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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
KAFKA_NUM_CONTAINERS=${KAFKA_NUM_CONTAINERS:-14}
TC_PATHS=${TC_PATHS:-${ESK_TEST_YML:-./kafkatest/}}
TC_GENERAL_MIRROR_URL=${TC_GENERAL_MIRROR_URL:-""}
TC_BASE_IMAGE=${TC_BASE_IMAGE:-"automqinc/kos_e2e_base:3.9.1"}
REBUILD=${REBUILD:f}
DUCKER_TEST_OPTIONS=${DUCKER_TEST_OPTIONS:-""}

die() {
    echo $@
    exit 1
}

if [[ "$_DUCKTAPE_OPTIONS" == *"kafka_mode"* && "$_DUCKTAPE_OPTIONS" == *"native"* ]]; then
    export KAFKA_MODE="native"
else
    export KAFKA_MODE="jvm"
fi

if [ "$REBUILD" == "t" ]; then
    ./gradlew clean systemTestLibs
    if [ "$KAFKA_MODE" == "native" ]; then
        ./gradlew clean releaseTarGz
    fi
fi

if ${SCRIPT_DIR}/ducker-ak ssh | grep -q '(none)'; then
    if [ -n "${TC_GENERAL_MIRROR_URL}" ]; then
        ${SCRIPT_DIR}/ducker-ak up -n "${KAFKA_NUM_CONTAINERS}" --general-mirror-url "${TC_GENERAL_MIRROR_URL}" --jdk "${TC_BASE_IMAGE}" -m "${KAFKA_MODE}" || die "ducker-ak up failed"
    else
        ${SCRIPT_DIR}/ducker-ak up -n "${KAFKA_NUM_CONTAINERS}" --jdk "${TC_BASE_IMAGE}" -m "${KAFKA_MODE}" || die "ducker-ak up failed"
    fi
fi

[[ -n ${_DUCKTAPE_OPTIONS} ]] && _DUCKTAPE_OPTIONS="-- ${_DUCKTAPE_OPTIONS}"

if [ -n "${DUCKER_TEST_OPTIONS}" ]; then
    ${SCRIPT_DIR}/ducker-ak test "${DUCKER_TEST_OPTIONS}" ${TC_PATHS} ${_DUCKTAPE_OPTIONS} || die "ducker-ak test failed"
else
    ${SCRIPT_DIR}/ducker-ak test ${TC_PATHS} ${_DUCKTAPE_OPTIONS} || die "ducker-ak test failed"
fi
