# DevBox - AutoMQ Local Development Environment

Zero-configuration local dev environment for AutoMQ. One command to start a Kafka cluster with S3 storage (MinIO), JDWP debugging, and optional features.

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) command runner
- JDK 17+

## Quick Start

```bash
cd devbox
just start-build        # Build + single node
just start-build 3      # Build + 3-node cluster
```

After first build, iterate faster with:
```bash
just start              # Single node (skip build)
just start 3            # 3-node cluster (skip build)
```

DevBox automatically generates a stable CLUSTER_ID, node configs, and starts MinIO + AutoMQ in Docker with JDWP enabled.

## Commands

### Lifecycle

| Command | Description |
|---------|-------------|
| `just start` | Single node (skip build) |
| `just start 3` | 3-node cluster |
| `just start 1 telemetry` | Single node + OpenTelemetry metrics |
| `just start 5 tabletopic` | 5-node + TableTopic |
| `just start-build` | Build + single node |
| `just start-build 3 tabletopic analytics` | Build + full stack |
| `just stop` | Stop all services |
| `just restart` | Rebuild and restart |
| `just status` | Show service status |
| `just logs` | Tail all logs |
| `just logs 0` | Tail node 0 logs |

### Bin (direct passthrough)

Run any `bin/` command inside a node container:

```bash
just bin kafka-topics.sh --bootstrap-server localhost:9092 --list
just bin kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type brokers
just bin --node 1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### Shortcuts

| Command | Description |
|---------|-------------|
| `just topic-list` | List topics |
| `just topic-create my-topic` | Create topic (16 partitions) |
| `just topic-create my-topic --partitions 8` | Custom partitions |
| `just topic-describe my-topic` | Describe topic |
| `just produce my-topic` | Interactive producer |
| `just consume my-topic --from-beginning` | Consumer |

### Chaos

| Command | Description |
|---------|-------------|
| `just chaos-delay` | 200ms delay on node 0 |
| `just chaos-delay 500 node-1` | 500ms delay on node 1 |
| `just chaos-loss 10` | 10% packet loss on node 0 |
| `just chaos-reset` | Remove all rules |
| `just chaos-status` | Show rules |

## Port Allocation

| Node | Kafka | Controller | JDWP |
|------|-------|------------|------|
| 0 | 9092 | 9093 | 5005 |
| 1 | 19092 | 19093 | 5006 |
| 2 | 29092 | 29093 | 5007 |
| 3 | 39092 | 39093 | 5008 |
| 4 | 49092 | 49093 | 5009 |

Other services:
- MinIO Console: http://localhost:9001 (admin/password)
- Schema Registry: http://localhost:8081 (with tabletopic)
- Iceberg REST: http://localhost:8181 (with tabletopic)
- Spark: http://localhost:8888 (with analytics)
- Trino: http://localhost:8090 (with analytics)

## IDEA Remote Debugging

1. Run → Edit Configurations → + → Remote JVM Debug
2. Host: `localhost`, Port: `5005` (node 0)
3. Start DevBox, then attach debugger

## Configuration

5-layer config system (low → high priority):

1. `config/defaults.properties` — MinIO, KRaft basics
2. Role — process.roles, listeners (auto: controller for node 0-2, broker-only for 3+)
3. Node identity — node.id, cluster.id, quorum.voters, advertised.listeners (auto)
4. `config/features/*.properties` — tabletopic, zerozone, telemetry (applied via start args)
5. `config/custom.properties` — your overrides, highest priority (gitignored)

To customize, create `config/custom.properties`:

```properties
num.partitions=4
log.retention.hours=24
```

