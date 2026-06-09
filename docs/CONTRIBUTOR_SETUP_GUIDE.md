# AutoMQ Contributor Setup Guide

Welcome! This guide helps first-time contributors set up a working local AutoMQ development environment, run builds/tests, and follow the expected contribution workflow.

## Prerequisites

Before you start, make sure your machine has the following installed and configured:

- **Java 17+**
  - Verify: `java -version`
- **Docker 20.x+**
  - Verify: `docker --version`
- **Docker Compose v2**
  - Use `docker compose` syntax (not `docker-compose`)
- **Docker resource allocation**
  - At least **4 GB RAM** allocated to Docker
- **Required ports available**
  - Ports **9092** and **9000** must be free
  - Verify: `lsof -i :9092 && lsof -i :9000`

## Fork & Clone

1. Fork the repository from: <https://github.com/AutoMQ/automq>
2. Clone your fork:

```bash
git clone https://github.com/<your-username>/automq.git
cd automq
git remote add upstream https://github.com/AutoMQ/automq.git
```

## Start Local Environment

1. Download the Docker Compose file:

```bash
curl -O https://raw.githubusercontent.com/AutoMQ/automq/refs/tags/1.5.5/docker/docker-compose.yaml
```

2. Start the local environment:

```bash
docker compose -f docker-compose.yaml up -d
```

3. Wait ~30 seconds, then verify containers are healthy:

```bash
docker compose -f docker-compose.yaml ps
```

4. Verify end-to-end with a producer performance test:

```bash
docker run --network automq_net automqinc/automq:latest /bin/bash -c \
"/opt/automq/kafka/bin/kafka-producer-perf-test.sh --topic test-topic --num-records=1024000 \
--throughput 5120 --record-size 1024 \
--producer-props bootstrap.servers=server1:9092 linger.ms=100 batch.size=524288"
```

5. Tear down when done:

```bash
docker compose -f docker-compose.yaml down
```

## Build from Source

Use the Gradle wrapper included in the repository.

- Full build (no tests):

```bash
./gradlew build -x test
```

- Run all tests:

```bash
./gradlew test
```

- Run module tests (`s3stream`):

```bash
./gradlew :s3stream:test
```

> **Note:** Never install Gradle manually for this project — always use `./gradlew`.

## Key Modules

| Module | Description |
| --- | --- |
| `s3stream/` | Core S3 storage engine, WAL, caching. Heart of AutoMQ. |
| `core/` | Kafka broker core, forked from Apache Kafka. |
| `automq-metrics/` | Prometheus and OpenTelemetry metrics export. |
| `automq-shell/` | CLI tooling for cluster management. |
| `docker/` | Docker Compose setups for local dev. |
| `examples/` | Usage examples and integration demos. |

## Contribution Workflow

1. Browse open issues at <https://github.com/AutoMQ/automq/issues>
2. Claim by commenting `/assign` on the issue thread
3. Create a branch:

```bash
git checkout -b feat/your-feature-name upstream/main
```

4. Make changes following existing code style (Checkstyle enforced)
5. Sync before PR:

```bash
git fetch upstream && git rebase upstream/main
```

6. Open PR against `AutoMQ/automq:main` and fill the PR template completely
7. Sign the CLA when prompted (required, automated on first PR)
8. Respond to review feedback by pushing new commits (do not force-push after review starts)

## Community & Support

| Channel | Best Used For |
| --- | --- |
| GitHub Issues | Bugs, feature requests |
| Slack (<https://go.automq.com/slack>) | Real-time questions, introductions |
| DeepWiki (<https://deepwiki.com/AutoMQ/automq>) | Codebase architecture exploration |
| Gurubase (<https://gurubase.io/g/automq>) | AI Q&A about the codebase |

## Commands Cheat Sheet

| Action | Command |
| --- | --- |
| Start cluster | `docker compose -f docker-compose.yaml up -d` |
| Stop cluster | `docker compose -f docker-compose.yaml down` |
| Full build | `./gradlew build -x test` |
| Run tests | `./gradlew test` |
| Module tests | `./gradlew :s3stream:test` |
| Sync upstream | `git fetch upstream && git rebase upstream/main` |
| Claim issue | Comment `/assign` on the issue thread |
