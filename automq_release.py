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
        print("*** First command failure occurred here.      ***")
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


def is_tag_valid(tag):
    """Check whether tag is valid"""
    pattern = r'^\d+\.\d+\.\d+(-rc)?[0-9]*$'
    if re.match(pattern, tag):
        return True
    else:
        return False


def get_project_path():
    # Path object for the current file
    current_file = Path(__file__)
    # Get the directory of the current file
    return current_file.parent


def do_release(tag_version):
    new_branch = "release-%s" % tag_version
    cmd("Checking out to release branch", 'git checkout -b %s' % new_branch)
    print("Updating version numbers")
    release_version = tag_version.split("-")[0]

    print("updating docker compose")
    regexReplace("docker/docker-compose.yaml", "image: automqinc/kafka:.*$", "image: automqinc/kafka:%s" % tag_version)

    cmd("Committing changes", ["git", "commit", "-a", "-m", "ci: Bump version to %s" % tag_version], allow_failure=True)
    cmd("Pushing changes", ["git", "push", "origin", new_branch], allow_failure=True)

    cmd("Tagging %s" % tag_version, ["git", "tag", "-a", tag_version, "-m", "release %s" % tag_version, "-s"])
    cmd("Pushing tag %s" % tag_version, ["git", "push", "origin", tag_version])


def clean_local_tags():
    tags = cmd_output('git tag -l').split()
    cmd("Cleaning local tags", ['git', 'tag', '-d'] + tags)


def check_before_started(tag_version):
    check_tools(["git"])
    cmd("Verifying that you have no unstaged git changes", 'git diff --exit-code --quiet')
    cmd("Verifying that you have no staged git changes", 'git diff --cached --exit-code --quiet')
    starting_branch = cmd_output('git rev-parse --abbrev-ref HEAD').strip()
    if starting_branch != main_branch:
        fail("You must run this script from the %s branch. current: %s" % (main_branch, starting_branch))
    cmd("Pull latest code", 'git pull --rebase origin %s' % main_branch)

    # Validate that the release doesn't already exist
    clean_local_tags()
    cmd("Fetching tags from upstream", 'git fetch --tags')
    tags = cmd_output('git tag').split()

    if tag_version in tags:
        fail("The specified version has already been tagged and released.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='release AutoMQ Kafka')
    parser.add_argument("--tag", required=True, type=str,
                        help="release tag, which should be in the format of 'x.y.z' or 'x.y.z-rcN'")

    # get tag from command line
    args = parser.parse_args()
    tag = args.tag
    print("=== try to release %s ===" % tag)
    if not is_tag_valid(tag):
        fail("Invalid tag")
    check_before_started(tag)
    do_release(tag)
    print("=== release %s done ===" % tag)
    print("=== please create a PR to merge release branch to %s ===" % main_branch)
