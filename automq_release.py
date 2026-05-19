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

#!/usr/bin/env python3

import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
import argparse
from shutil import which

MAIN_BRANCH = "develop"


def fail(message: str) -> None:
    """Print error message and exit."""
    print(message)
    sys.exit(1)


def print_output(output: str | None) -> None:
    """Pretty print command output."""
    if not output:
        return
    for line in output.split("\n"):
        print(">", line)


def run_cmd(action: str, command, *args, **kwargs):
    """
    Run a shell command with logging, retries, and optional failure allowance.
    """
    if isinstance(command, str) and not kwargs.get("shell", False):
        command = command.split()

    allow_failure = kwargs.pop("allow_failure", False)
    retries_left = kwargs.pop("num_retries", 0)

    stdin_log = ""
    if "stdin" in kwargs and isinstance(kwargs["stdin"], str):
        stdin_log = "--> " + kwargs["stdin"]
        stdin_file = tempfile.TemporaryFile()
        stdin_file.write(kwargs["stdin"].encode("utf-8"))
        stdin_file.seek(0)
        kwargs["stdin"] = stdin_file

    print(action, command, stdin_log)

    try:
        output = subprocess.check_output(command, *args, stderr=subprocess.STDOUT, **kwargs)
        print_output(output.decode("utf-8"))

    except subprocess.CalledProcessError as error:
        print_output(error.output.decode("utf-8"))

        if retries_left > 0:
            kwargs["num_retries"] = retries_left - 1
            kwargs["allow_failure"] = allow_failure
            print(f"Retrying... {retries_left - 1} remaining retries")
            time.sleep(4.0 / (retries_left + 1))
            return run_cmd(action, command, *args, **kwargs)

        if allow_failure:
            return

        print("*" * 49)
        print("***    First command failure occurred here.   ***")
        print("*" * 49)
        fail("")


def get_cmd_output(command, *args, **kwargs) -> str:
    """Run command and return decoded output."""
    if isinstance(command, str):
        command = command.split()
    return subprocess.check_output(command, *args, stderr=subprocess.STDOUT, **kwargs).decode("utf-8")


def replace_line_start(path: str, pattern: str, replacement: str) -> None:
    """Replace lines that start with a specific pattern."""
    updated_lines = []
    with open(path, "r") as file:
        for line in file:
            updated_lines.append((replacement + "\n") if line.startswith(pattern) else line)

    with open(path, "w") as file:
        file.writelines(updated_lines)


def regex_replace(path: str, pattern: str, replacement: str) -> None:
    """Regex-based replacement for each line in a file."""
    updated_lines = []
    with open(path, "r") as file:
        for line in file:
            updated_lines.append(re.sub(pattern, replacement, line))

    with open(path, "w") as file:
        file.writelines(updated_lines)


def is_tool_available(name: str) -> bool:
    """Check whether a tool exists in system PATH."""
    return which(name) is not None


def check_required_tools(tools: list[str]) -> None:
    """Ensure required CLI tools are installed."""
    for tool in tools:
        if not is_tool_available(tool):
            raise Exception(f"{tool} is not installed")


def is_valid_kafka_tag(tag: str) -> bool:
    """Validate Kafka release tag format."""
    return re.match(r"^\d+\.\d+\.\d+(-rc\d+)?$", tag) is not None


def is_valid_s3stream_tag(tag: str) -> bool:
    """Validate s3stream release tag format."""
    return re.match(r"^\d+\.\d+\.\d+-s3stream(-rc\d+)?$", tag) is not None


def get_project_path() -> Path:
    """Return project root path."""
    return Path(__file__).parent


def create_release(tag: str, s3stream_tag: str) -> str:
    """Create release branch, update versions, and push tags."""
    new_branch = f"release-{tag}-{int(time.time())}"

    run_cmd("Checking out to release branch", f"git checkout --quiet -b {new_branch}")

    print("Updating docker compose")
    regex_replace("docker/docker-compose.yaml", r"image: automqinc/automq:.*$", f"image: automqinc/automq:{tag}")
    replace_line_start("gradle/dependencies.gradle", '    branch = "main"', f'    require "{s3stream_tag}"')

    run_cmd("Committing changes", ["git", "commit", "--all", "--gpg-sign", "--signoff",
                                   "--message", f"ci: bump version to {tag}"])
    run_cmd("Pushing changes", ["git", "push", "--quiet", "origin", new_branch])

    run_cmd(f"Tagging {tag}", ["git", "tag", "--sign", "--annotate", tag, "--message", f"release {tag}"])
    run_cmd(f"Pushing tag {tag}", ["git", "push", "--quiet", "origin", tag])

    return new_branch


def tag_exists_locally(tag: str) -> bool:
    """Check if a Git tag exists locally."""
    return len(get_cmd_output(f"git tag --list {tag}").strip()) > 0


def pre_release_checks(tag: str, s3stream_tag: str) -> None:
    """Run all validations before starting release."""
    check_required_tools(["git"])

    run_cmd("Fetching latest code", "git fetch origin")
    run_cmd("Checking workspace clean", "git diff-index --quiet HEAD --")

    current_branch = get_cmd_output("git rev-parse --abbrev-ref HEAD").strip()
    if current_branch != MAIN_BRANCH:
        fail(f"You must run this script on the {MAIN_BRANCH} branch. current: {current_branch}")

    run_cmd(f"Checking branch {MAIN_BRANCH} up-to-date",
            f"git diff origin/{MAIN_BRANCH}...{MAIN_BRANCH} --quiet")

    run_cmd(f"Checking tag {s3stream_tag} exists",
            f"git ls-remote --quiet --exit-code --tags git@github.com:AutoMQ/automq-for-rocketmq.git {s3stream_tag}")

    run_cmd(f"Checking tag {tag} not exists in remote",
            f"! git ls-remote --quiet --exit-code --tags origin {tag}", shell=True)

    print(f"Checking tag {tag} not exists in local")
    if tag_exists_locally(tag):
        fail(f"Tag {tag} already exists. Please delete it before running this script.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="release AutoMQ Kafka")
    parser.add_argument("--kafka-tag", required=True, type=str,
                        help="Format: x.y.z or x.y.z-rcN")
    parser.add_argument("--s3stream-tag", required=True, type=str,
                        help="Format: x.y.z-s3stream or x.y.z-s3stream-rcN")

    args = parser.parse_args()
    tag = args.kafka_tag
    s3stream_tag = args.s3stream_tag

    print(f"=== try to release {tag} ===")

    if not is_valid_kafka_tag(tag):
        fail(f"Invalid tag {tag}")
    if not is_valid_s3stream_tag(s3stream_tag):
        fail(f"Invalid s3stream tag {s3stream_tag}")

    pre_release_checks(tag, s3stream_tag)
    branch = create_release(tag, s3stream_tag)

    print(f"=== release {tag} done ===")
    print(f"Please create a PR to merge release branch to {MAIN_BRANCH} visiting:")
    print(f"    https://github.com/AutoMQ/automq-for-kafka/pull/new/{MAIN_BRANCH}...{branch}")
