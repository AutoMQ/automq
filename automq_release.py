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

# !/usr/bin/python3

import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
import argparse

main_branch = "develop"


def fail(msg):
    print(msg)
    sys.exit(1)


def print_output(output):
    if output is None or len(output) == 0:
        return
    for line in output.split('\n'):
        print(">", line)


def cmd(action, cmd_arg, *args, **kwargs):
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

    print(action, cmd_arg, stdin_log)
    try:
        output = subprocess.check_output(cmd_arg, *args, stderr=subprocess.STDOUT, **kwargs)
        print_output(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        print_output(e.output.decode('utf-8'))

        if num_retries > 0:
            kwargs['num_retries'] = num_retries - 1
            kwargs['allow_failure'] = allow_failure
            print("Retrying... %d remaining retries" % (num_retries - 1))
            time.sleep(4. / (num_retries + 1))  # e.g., if retries=3, sleep for 1s, 1.3s, 2s
            return cmd(action, cmd_arg, *args, **kwargs)

        if allow_failure:
            return

        print("*************************************************")
        print("***    First command failure occurred here.   ***")
        print("*************************************************")
        fail("")


def cmd_output(cmd, *args, **kwargs):
    if isinstance(cmd, str):
        cmd = cmd.split()
    return subprocess.check_output(cmd, *args, stderr=subprocess.STDOUT, **kwargs).decode('utf-8')


def replace(path, pattern, replacement):
    updated = []
    with open(path, 'r') as f:
        for line in f:
            updated.append((replacement + '\n') if line.startswith(pattern) else line)

    with open(path, 'w') as f:
        for line in updated:
            f.write(line)


def regexReplace(path, pattern, replacement):
    updated = []
    with open(path, 'r') as f:
        for line in f:
            updated.append(re.sub(pattern, replacement, line))

    with open(path, 'w') as f:
        for line in updated:
            f.write(line)


def is_tool(name):
    """Check whether `name` is on PATH and marked as executable."""

    # from whichcraft import which
    from shutil import which

    return which(name) is not None


def check_tools(tools):
    """Check whether tools are available in the system"""
    for tool in tools:
        if not is_tool(tool):
            raise Exception("%s is not installed" % tool)


def is_valid_kafka_tag(tag):
    """Check whether the tag is a valid kafka tag"""
    pattern = r'^\d+\.\d+\.\d+(-rc\d+)?$'
    return re.match(pattern, tag) is not None


def is_valid_s3stream_tag(tag):
    """Check whether the tag is a valid s3stream tag"""
    pattern = r'^\d+\.\d+\.\d+-s3stream(-rc\d+)?$'
    return re.match(pattern, tag) is not None


def get_project_path():
    # Path object for the current file
    current_file = Path(__file__)
    # Get the directory of the current file
    return current_file.parent


def do_release(tag, s3stream_tag):
    new_branch = f"release-{tag}-{int(time.time())}"
    cmd(f"Checking out to release branch", f'git checkout --quiet -b {new_branch}')

    print("Updating docker compose")
    regexReplace("docker/docker-compose.yaml", "image: automqinc/automq:.*$", f"image: automqinc/automq:{tag}")
    replace("gradle/dependencies.gradle", '    branch = "main"', f'    require "{s3stream_tag}"')

    cmd("Committing changes", ["git", "commit", "--all", "--gpg-sign", "--signoff", "--message", f"ci: bump version to {tag}"])
    cmd("Pushing changes", ["git", "push", "--quiet", "origin", new_branch])

    cmd("Tagging %s" % tag, ["git", "tag", "--sign", "--annotate", tag, "--message", f"release {tag}"])
    cmd("Pushing tag %s" % tag, ["git", "push", "--quiet", "origin", tag])

    return new_branch


def tag_exists(tag):
    maybe_tag = cmd_output(f'git tag --list {tag}').strip()
    return len(maybe_tag) > 0


def check_before_started(tag, s3stream_tag):
    check_tools(["git"])
    cmd("Fetching latest code", 'git fetch origin')
    cmd("Checking workspace clean", 'git diff-index --quiet HEAD --')
    starting_branch = cmd_output('git rev-parse --abbrev-ref HEAD').strip()
    if starting_branch != main_branch:
        fail(f"You must run this script on the {main_branch} branch. current: {starting_branch}")
    cmd(f"Checking branch {main_branch} up-to-date", f'git diff origin/{main_branch}...{main_branch} --quiet')
    cmd(f"Checking tag {s3stream_tag} exists", f'git ls-remote --quiet --exit-code --tags git@github.com:AutoMQ/automq-for-rocketmq.git {s3stream_tag}')
    cmd(f"Checking tag {tag} not exists in remote", f'! git ls-remote --quiet --exit-code --tags origin {tag}', shell=True)
    print(f"Checking tag {tag} not exists in local")
    if tag_exists(tag):
        fail(f"Tag {tag} already exists. Please delete it before running this script.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='release AutoMQ Kafka')
    parser.add_argument("--kafka-tag", required=True, type=str,
                        help="release tag, which should be in the format of 'x.y.z' or 'x.y.z-rcN', e.g., 1.2.3 or 11.22.33-rc44")
    parser.add_argument("--s3stream-tag", required=True, type=str,
                        help="related s3stream tag, which should have been pushed to the AutoMQ/automq-for-rocketmq repository,\
                            and should be in the format of 'x.y.z-s3stream' or 'x.y.z-s3stream-rcN', e.g., 1.2.3-s3stream or 11.22.33-s3stream-rc44")

    # get tag from command line
    args = parser.parse_args()
    tag = args.kafka_tag
    s3stream_tag = args.s3stream_tag
    print(f"=== try to release {tag} ===")
    if not is_valid_kafka_tag(tag):
        fail(f"Invalid tag {tag}")
    if not is_valid_s3stream_tag(s3stream_tag):
        fail(f"Invalid s3stream tag {s3stream_tag}")
    check_before_started(tag, s3stream_tag)
    branch = do_release(tag, s3stream_tag)
    print(f"=== release {tag} done ===")
    print(f"Please create a PR to merge release branch to {main_branch} visiting:")
    print(f"    https://github.com/AutoMQ/automq-for-kafka/pull/new/{main_branch}...{branch}")
