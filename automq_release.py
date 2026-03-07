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

#!/usr/bin/python3

"""Release automation script for AutoMQ Kafka.

Handles version tagging, branch creation, and release artifact preparation
for both Kafka and S3Stream components.
"""

import argparse
import logging
import re
import subprocess
import sys
import tempfile
import time
from shutil import which
from typing import Optional

logger = logging.getLogger(__name__)

main_branch = "develop"


def fail(msg: str) -> None:
    logger.error(msg)
    sys.exit(1)


def print_output(output: Optional[str]) -> None:
    if not output:
        return
    for line in output.split('\n'):
        logger.info("> %s", line)


def cmd(action: str, cmd_arg: "str | list[str]", *args, **kwargs) -> None:
    if isinstance(cmd_arg, str) and not kwargs.get("shell", False):
        cmd_arg = cmd_arg.split()
    allow_failure = kwargs.pop("allow_failure", False)
    num_retries = kwargs.pop("num_retries", 0)

    stdin_log = ""
    if "stdin" in kwargs and isinstance(kwargs["stdin"], str):
        stdin_log = "--> " + kwargs["stdin"]
        stdin = tempfile.TemporaryFile()
        stdin.write(kwargs["stdin"].encode('utf-8'))
        stdin.seek(0)
        kwargs["stdin"] = stdin

    logger.info("%s %s %s", action, cmd_arg, stdin_log)
    try:
        output = subprocess.check_output(cmd_arg, *args, stderr=subprocess.STDOUT, **kwargs)
        print_output(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print_output(e.output.decode('utf-8'))

        if num_retries > 0:
            kwargs['num_retries'] = num_retries - 1
            kwargs['allow_failure'] = allow_failure
            logger.warning("Retrying... %d remaining retries", num_retries - 1)
            time.sleep(4. / (num_retries + 1))
            return cmd(action, cmd_arg, *args, **kwargs)

        if allow_failure:
            return

        logger.error("*************************************************")
        logger.error("***    First command failure occurred here.   ***")
        logger.error("*************************************************")
        fail("")


def run_cmd(command: "str | list[str]", *args, **kwargs) -> str:
    """Run a command and return its decoded stdout output."""
    if isinstance(command, str):
        command = command.split()
    return subprocess.check_output(command, *args, stderr=subprocess.STDOUT, **kwargs).decode('utf-8')


def replace(path: str, pattern: str, replacement: str) -> None:
    updated = []
    with open(path, 'r') as f:
        for line in f:
            updated.append((replacement + '\n') if line.startswith(pattern) else line)

    with open(path, 'w') as f:
        for line in updated:
            f.write(line)


def regex_replace(path: str, pattern: str, replacement: str) -> None:
    updated = []
    with open(path, 'r') as f:
        for line in f:
            updated.append(re.sub(pattern, replacement, line))

    with open(path, 'w') as f:
        for line in updated:
            f.write(line)


def is_tool(name: str) -> bool:
    """Check whether `name` is on PATH and marked as executable."""
    return which(name) is not None


def check_tools(tools: "list[str]") -> None:
    """Check whether required tools are available in the system."""
    for tool in tools:
        if not is_tool(tool):
            raise Exception(f"{tool} is not installed")


def is_valid_kafka_tag(tag: str) -> bool:
    """Check whether the tag is a valid Kafka tag (e.g. '1.2.3' or '1.2.3-rc1')."""
    pattern = r'^\d+\.\d+\.\d+(-rc\d+)?$'
    return re.match(pattern, tag) is not None


def is_valid_s3stream_tag(tag: str) -> bool:
    """Check whether the tag is a valid S3Stream tag (e.g. '1.2.3-s3stream' or '1.2.3-s3stream-rc1')."""
    pattern = r'^\d+\.\d+\.\d+-s3stream(-rc\d+)?$'
    return re.match(pattern, tag) is not None


def do_release(tag: str, s3stream_tag: str) -> str:
    new_branch = f"release-{tag}-{int(time.time())}"
    cmd("Checking out to release branch", f'git checkout --quiet -b {new_branch}')

    logger.info("Updating docker compose")
    regex_replace("docker/docker-compose.yaml", "image: automqinc/automq:.*$", f"image: automqinc/automq:{tag}")
    replace("gradle/dependencies.gradle", '    branch = "main"', f'    require "{s3stream_tag}"')

    cmd("Committing changes", ["git", "commit", "--all", "--gpg-sign", "--signoff", "--message", f"ci: bump version to {tag}"])
    cmd("Pushing changes", ["git", "push", "--quiet", "origin", new_branch])

    cmd(f"Tagging {tag}", ["git", "tag", "--sign", "--annotate", tag, "--message", f"release {tag}"])
    cmd(f"Pushing tag {tag}", ["git", "push", "--quiet", "origin", tag])

    return new_branch


def tag_exists(tag: str) -> bool:
    maybe_tag = run_cmd(f'git tag --list {tag}').strip()
    return len(maybe_tag) > 0


def check_before_started(tag: str, s3stream_tag: str) -> None:
    check_tools(["git"])
    cmd("Fetching latest code", 'git fetch origin')
    cmd("Checking workspace clean", 'git diff-index --quiet HEAD --')
    starting_branch = run_cmd('git rev-parse --abbrev-ref HEAD').strip()
    if starting_branch != main_branch:
        fail(f"You must run this script on the {main_branch} branch. current: {starting_branch}")
    cmd(f"Checking branch {main_branch} up-to-date", f'git diff origin/{main_branch}...{main_branch} --quiet')
    cmd(f"Checking tag {s3stream_tag} exists", f'git ls-remote --quiet --exit-code --tags git@github.com:AutoMQ/automq-for-rocketmq.git {s3stream_tag}')
    cmd(f"Checking tag {tag} not exists in remote", f'! git ls-remote --quiet --exit-code --tags origin {tag}', shell=True)
    logger.info("Checking tag %s not exists in local", tag)
    if tag_exists(tag):
        fail(f"Tag {tag} already exists. Please delete it before running this script.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description='Release AutoMQ Kafka')
    parser.add_argument("--kafka-tag", required=True, type=str,
                        help="release tag in the format 'x.y.z' or 'x.y.z-rcN', e.g. 1.2.3 or 11.22.33-rc44")
    parser.add_argument("--s3stream-tag", required=True, type=str,
                        help="related s3stream tag that should already exist in AutoMQ/automq-for-rocketmq, "
                             "in the format 'x.y.z-s3stream' or 'x.y.z-s3stream-rcN', e.g. 1.2.3-s3stream")

    args = parser.parse_args()
    tag = args.kafka_tag
    s3stream_tag = args.s3stream_tag
    logger.info("=== try to release %s ===", tag)
    if not is_valid_kafka_tag(tag):
        fail(f"Invalid tag {tag}")
    if not is_valid_s3stream_tag(s3stream_tag):
        fail(f"Invalid s3stream tag {s3stream_tag}")
    check_before_started(tag, s3stream_tag)
    branch = do_release(tag, s3stream_tag)
    logger.info("=== release %s done ===", tag)
    logger.info("Please create a PR to merge release branch to %s visiting:", main_branch)
    logger.info("    https://github.com/AutoMQ/automq-for-kafka/pull/new/%s...%s", main_branch, branch)
