Releasing Apache Kafka
======================

This directory contains the tools used to publish a release.

# Requirements

* python 3.12
* git
* gpg 2.4
* sftp

The full instructions for producing a release are available in
https://cwiki.apache.org/confluence/display/KAFKA/Release+Process.


# License manifest

`LICENSE-binary` is the human-readable redistribution manifest for
the production binary tarball. It is not an SBOM. The manifest records the
third-party JARs actually shipped under `libs/`, grouped by license, while
`licenses/` contains consolidated license text and the JARs retain upstream
license and notice resources where provided. A machine-readable SPDX or
CycloneDX SBOM should be generated separately from the exact release artifact.

The maintenance workflow follows Apache Kafka's practice:

1. When a dependency or release packaging rule changes, inspect the resolved
   runtime graph and update `LICENSE-binary` in the same change.
2. Build the production tarball with `releaseTarGz`. The task places
   `LICENSE-binary` at the package root as `LICENSE`, and copies
   `licenses/` and `NOTICE-binary` into the package.
3. Run the final-artifact check:

   ```
   python3 ./committer-tools/verify_license.py
   ```

   For an already-built tarball, use
   `python3 ./committer-tools/verify_license.py --skip-build`.
   The check compares the actual third-party `libs/*.jar` names and versions
   with the package `LICENSE`, matching Kafka's inventory check.
4. For every external release artifact, archive an SPDX or CycloneDX SBOM
   bound to the artifact checksum. Do not use the SBOM as a replacement for
   `LICENSE` or `NOTICE`.

Apache Kafka follows the same separation: its root `LICENSE-binary` is the
manually reviewed input, `build.gradle` maps it to the binary package root as
`LICENSE`, and `committer-tools/verify_license.py` compares the final
`libs/*.jar` inventory with that file. Kafka keeps the manual update because
license metadata is not uniform across Maven artifacts; see
KAFKA-12622 for the rationale. `NOTICE-binary` remains a separate attribution
document.

For the source-backed upstream workflow and its exact file/command references,
see [Apache Kafka LICENSE-binary workflow research](apache-kafka-license-workflow.md).

Keep the source paths and command entry points aligned with Kafka when syncing
upstream changes. Resolve conflicts only for AutoMQ's additional dependencies,
project artifact names, tarball prefixes, and version-token parsing; do not
create a second verifier or a second license manifest format.

The check is a release blocker. It validates inventory and packaging
consistency; legal review is still required for new license families,
multi-license components, and changes to `NOTICE-binary`.

`releaseE2ETar` is an internal test package and includes test-only JARs. It is
not covered by the production manifest check and must not be treated as an
external binary release. If that package becomes externally distributable, it
needs its own exact-content license manifest and verification step.


# Setup

Create a virtualenv for python, activate it and install dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

# Usage

To start a release, first activate the virutalenv, and then run
the release script.

```
source .venv/bin/activate
```

You'll need to setup `PUSH_REMOTE_NAME` to refer to
the git remote for `apache/kafka`.

```
export PUSH_REMOTE_NAME=<value>
```

It should be the value shown with this command:

```
git remote -v | grep -w 'github.com' | grep -w 'apache/kafka' | grep -w '(push)' | awk '{print $1}'
```

Then start the release script:

```
python release.py
```

Should you encounter some problem, where re-running the script doesn't work, look at the following steps:

- The script remembers data inputted previously if you need to correct it, it is saved under the
`.release-settings.json` file in the `release` folder.
- If the script is interrupted you might need to manually delete the tag named after the release candidate name and
branch named after the release version.
