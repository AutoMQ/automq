#!/usr/bin/env python3

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

"""Verify that the production tarball and LICENSE-binary describe the same JARs."""

# Upstream reference: https://github.com/apache/kafka/blob/trunk/committer-tools/verify_license.py
# This follows Apache Kafka's committer-tools/verify_license.py. The comparison
# semantics are unchanged; local code only adapts release input discovery,
# project artifact filtering, and AutoMQ dependency token boundaries.

import argparse
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


# Kafka compares the first token on each manifest bullet. Keep that same
# boundary while allowing Maven artifact names with arbitrary version suffixes.
LICENSE_DEP_PATTERN = re.compile(r"^\s*-\s+([^\s,]+)", re.MULTILINE)

# These are the project artifacts added by releaseTarGz. Do not exclude every
# kafka-* JAR: Confluent's kafka-avro and schema-registry artifacts are third-party.
PROJECT_ARTIFACT_PREFIXES = frozenset(
    {
        "automq-log-uploader",
        "automq-metrics",
        "automq-shell",
        "connect-api",
        "connect-basic-auth-extension",
        "connect-file",
        "connect-json",
        "connect-mirror",
        "connect-mirror-client",
        "connect-runtime",
        "connect-transforms",
        "kafka-clients",
        "kafka-group-coordinator",
        "kafka-group-coordinator-api",
        "kafka-log4j-appender",
        "kafka-metadata",
        "kafka-raft",
        "kafka-server",
        "kafka-server-common",
        "kafka-shell",
        "kafka-storage",
        "kafka-storage-api",
        "kafka-streams",
        "kafka-streams-examples",
        "kafka-streams-scala_2.13",
        "kafka-streams-test-utils",
        "kafka-tools",
        "kafka-tools-api",
        "kafka-transaction-coordinator",
        "kafka_2.13",
        "s3stream",
        "trogdor",
    }
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def build_release(root: Path) -> None:
    subprocess.run(
        ["./gradlew", "--no-daemon", "clean", "releaseTarGz"],
        cwd=root,
        check=True,
    )


def find_tarball(root: Path) -> Path:
    distributions = root / "core" / "build" / "distributions"
    candidates = sorted(
        (
            path
            for path in distributions.glob("*.tgz")
            if "site-docs" not in path.name
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No production tarball found in {distributions}")
    return candidates[0]


def safe_extract(tarball: Path, destination: Path) -> Path:
    with tarfile.open(tarball, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            target = (destination / member.name).resolve()
            if destination != target and destination not in target.parents:
                raise ValueError(f"Unsafe path in tarball: {member.name}")
        archive.extractall(destination)

    roots = {member.name.split("/", 1)[0] for member in members if member.name}
    if len(roots) != 1:
        raise ValueError(f"Expected one tarball root, found: {sorted(roots)}")
    return destination / roots.pop()


def is_project_artifact(stem: str) -> bool:
    # The release root may have a CI prefix, so derive ownership from the
    # artifact name instead of the directory name.
    return any(stem.startswith(f"{prefix}-") for prefix in PROJECT_ARTIFACT_PREFIXES)


def get_license_deps(license_path: Path) -> set[str]:
    text = license_path.read_text(encoding="utf-8")
    return set(LICENSE_DEP_PATTERN.findall(text))


def verify(release_root: Path) -> list[str]:
    license_path = release_root / "LICENSE"
    libs_path = release_root / "libs"
    if not license_path.is_file():
        return [f"Missing package LICENSE: {license_path}"]
    if not libs_path.is_dir():
        return [f"Missing package libs directory: {libs_path}"]

    jar_stems = {
        jar.stem
        for jar in libs_path.glob("*.jar")
        if not is_project_artifact(jar.stem)
    }
    manifest_set = get_license_deps(license_path)
    errors = []

    missing = sorted(jar_stems - manifest_set)
    extra = sorted(manifest_set - jar_stems)
    if missing:
        errors.append("JARs missing from LICENSE: " + ", ".join(missing))
    if extra:
        errors.append("LICENSE entries missing from package: " + ", ".join(extra))

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify the production tarball against its LICENSE manifest."
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Use the newest existing production tarball instead of rebuilding.",
    )
    args = parser.parse_args()
    root = project_root()

    if not args.skip_build:
        build_release(root)

    tarball = find_tarball(root)
    print(f"Verifying {tarball}")
    with tempfile.TemporaryDirectory(prefix="automq-license-") as temporary_directory:
        release_root = safe_extract(tarball, Path(temporary_directory))
        errors = verify(release_root)

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print("LICENSE manifest matches all third-party JARs in the production tarball.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
