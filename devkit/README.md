# DevKit - AutoMQ Local Development Environment

## Overview

DevKit is AutoMQ's local development environment. One-command Kafka cluster with MinIO S3 storage, JDWP debugging, chaos testing, and optional feature stacks.

**Core principle:** All commands run via `just`. Kafka commands execute inside containers — use shortcuts or `just bin` from the host.

**Supported node counts:** 1, 3, 4, 5. Node numbering starts at 0.

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) — `brew install just`
- `envsubst` — `brew install gettext` (macOS)
- JDK 17+ (for `just start-build` only)

## Quick Start

```bash
# First time: build image + code, then start
just start-build        # single node
just start-build 3      # 3-node cluster

# After first build
just start              # single node
just start 3            # 3-node cluster
just start 3 tabletopic # 3-node + Iceberg table topic
```

## Architecture

```
MinIO (S3 storage)
  └── mc (bucket init, one-shot)
        └── AutoMQ Nodes
              ├── node 0-2: controller + broker (KRaft voters)
              └── node 3+:  broker only
```

Node roles by count: a 4-node cluster has nodes 0-2 as controllers, node 3 as broker-only.

Optional feature stacks: `tabletopic` → Schema Registry + Iceberg REST Catalog, `analytics` → Spark + Trino.

## Decision: Which start command to use?

```
Java code changed + Dockerfile changed → just start-build
Java code changed only                 → just restart-build
Config files changed only              → just restart
Nothing changed                        → just start [N]
```

## Lifecycle

```bash
just start                      # Start (skip build), single node
just start 3 tabletopic         # Start 3-node + features
just start-build                # Build image & code, then start
just stop                       # Stop containers (keep data)
just clean                      # Stop + remove all volumes
just restart                    # Restart containers (no rebuild)
just restart-build              # Rebuild code & image, then restart
just status                     # Show container status
just logs                       # Tail all logs
just logs 0                     # Tail node-0 logs only
just shell                      # Enter node-0 shell
just shell 1                    # Enter node-1 shell
just build-image                # Rebuild Docker image only
just wait                       # Wait until all nodes healthy (auto-called by start)
```

> `just stop` keeps data. `just clean` removes all volumes.

## Topic Operations

```bash
just topic-list                                     # List all topics
just topic-create my-topic --partitions 16          # Create topic
just topic-describe my-topic                        # Describe topic
just produce my-topic                               # Interactive producer (stdin)
just consume my-topic --from-beginning              # Consume from beginning
```

## Consumer Group Operations

```bash
just group-list                                     # List all consumer groups
just group-describe my-group                        # Show offsets and lag per partition
just group-members my-group                         # Show active members and assignments
just group-reset my-group my-topic --to-earliest    # Reset to earliest offset
just group-reset my-group my-topic --to-latest      # Reset to latest offset
just group-reset my-group my-topic --to-offset 100  # Reset to specific offset
```

## Cluster Operations

```bash
just broker-list                # List brokers and supported API versions
just broker-config              # Describe all broker configs (cluster-wide)
just broker-config 0            # Describe broker 0 configs only
```

## Bin (direct passthrough)

Run any Kafka `bin/` script inside a node container. Commands run via `docker exec`, so `localhost:9092` refers to the broker inside the container.

```bash
just bin kafka-topics.sh --bootstrap-server localhost:9092 --list
just bin kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type brokers
just bin --node 1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

## Perf

```bash
# -1 = unlimited throughput
just perf-produce my-topic --num-records 100000 --record-size 1024 --throughput -1
just perf-consume my-topic --messages 100000
```

## Diagnostics

### Arthas (Java runtime diagnostics)

```bash
just arthas         # Attach to node-0 Kafka process
just arthas 1       # Attach to node-1

# Inside Arthas REPL:
# dashboard                              — CPU, memory, thread overview
# thread -n 3                            — top 3 busy threads
# watch <class> <method> returnObj       — watch method return values
# trace <class> <method>                 — trace call tree with timing
```

### JMX

JMX exposed on node-0 at port 9999 via [jmxterm](https://github.com/jiaqi/jmxterm) (pre-installed).

```bash
just jmx -e 'domains'                                                              # List all JMX domains
just jmx -e 'beans -d kafka.server'                                                # List beans in domain
just jmx -e 'info -b kafka.server:name=BrokerState,type=KafkaServer'              # View bean attributes
just jmx -e 'get -b kafka.server:name=BrokerState,type=KafkaServer Value'         # Broker state (3 = running)
just jmx -e 'get -b kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics Count'
just jmx --node 1 -e 'get -b kafka.server:name=BrokerState,type=KafkaServer Value'
```

### JDK tools

```bash
just shell      # Enter node-0 shell — jstack, jmap, jcmd available
just shell 1    # Enter node-1 shell
```

> To change JVM heap size, edit `KAFKA_HEAP_OPTS` in `justfile` (default: `-Xms1g -Xmx4g`).

## IDEA Remote Debugging

JDWP is enabled on all nodes at startup. Port mapping: node-0 → 5005, node-1 → 5006, node-2 → 5007, ...

```bash
# In IDEA: Run → Edit Configurations → + → Remote JVM Debug → localhost:5005
just start
# Then attach debugger
```

## Chaos Testing

Inject faults using `tc netem` (network delay/loss) and `iptables` (traffic blocking). `NET_ADMIN` capability is already set in docker-compose.yml.

**Always run `just chaos-reset` when done** — clears all tc rules, iptables rules, and unpauses MinIO.

### Node network

```bash
just chaos-delay                    # 200ms delay on node-0
just chaos-delay 500 node-1         # 500ms delay on node-1
just chaos-loss                     # 5% packet loss on node-0
just chaos-loss 10 node-1           # 10% packet loss on node-1
```

### S3 (MinIO) faults

```bash
# Targeted: only affects traffic between the specified node and MinIO
just chaos-s3-delay 500 node-0      # 500ms latency to S3 on node-0
just chaos-s3-loss 10 node-0        # 10% packet loss to S3 on node-0

# Global
just chaos-s3-down                  # Pause MinIO (S3 completely unavailable)
just chaos-s3-up                    # Resume MinIO

# Partition (iptables DROP)
just chaos-s3-partition node-1      # Isolate node-1 from S3
just chaos-s3-partition-reset       # Restore S3 connectivity only
```

### Node-to-node partition

```bash
just chaos-partition node-0 node-1  # Bidirectional isolation (iptables DROP)
just chaos-partition-reset          # Restore node connectivity only
```

### Reset & status

```bash
just chaos-reset                    # Remove ALL chaos rules (tc + iptables + unpause MinIO)
just chaos-status                   # Show active rules per node and MinIO state
```

### Scenarios

#### S3 slow response
```bash
just chaos-s3-delay 500 node-0
just produce my-topic
just chaos-reset
```

#### S3 outage
```bash
just chaos-s3-down
just logs
just chaos-s3-up
```

#### Single node S3 partition
```bash
just chaos-s3-partition node-1
just logs 1
just chaos-s3-partition-reset
```

#### Controller network partition
```bash
just start 3
just chaos-partition node-0 node-1
just logs
just chaos-partition-reset
```

#### Combined faults
```bash
just start 3
just chaos-delay 200 node-0
just chaos-s3-loss 10 node-0
just produce my-topic
just chaos-reset
```

## Features

Append feature names to `just start`. Multiple features can be combined.

```bash
just start 1 telemetry              # Single node + OTel metrics
just start 3 tabletopic             # 3-node + Iceberg table topic
just start 5 zerozone analytics     # 5-node + zone router + query engines
```

| Feature | What it enables | Extra services |
|---------|----------------|----------------|
| `tabletopic` | Iceberg table topic, REST catalog, schema registry | Schema Registry (:8081), Iceberg REST (:8181) |
| `zerozone` | Zone router, auto AZ assignment (round-robin: az-0/1/2) | — (config only) |
| `telemetry` | OTel metrics export via OTLP HTTP — **edit `config/features/telemetry.properties` to set collector endpoint before use** | — (config only) |
| `analytics` | Spark + Trino for querying Iceberg tables | Spark (:8888), Trino (:8090) |

## Port Allocation

| Node | Kafka | Controller | JDWP | JMX |
|------|-------|------------|------|-----|
| 0 | 9092 | 9093 | 5005 | 9999 |
| 1 | 19092 | 19093 | 5006 | — |
| 2 | 29092 | 29093 | 5007 | — |
| 3 | 39092 | 39093 | 5008 | — |
| 4 | 49092 | 49093 | 5009 | — |

Other services:
- MinIO Console: http://localhost:9001 (admin/password)
- Schema Registry: http://localhost:8081 (with tabletopic)
- Iceberg REST: http://localhost:8181 (with tabletopic)
- Spark: http://localhost:8888 (with analytics)
- Trino: http://localhost:8090 (with analytics)

## Configuration

5-layer system — later layers override earlier ones. Each layer is a template rendered via `envsubst` before Docker starts. Final merged config: `.devkit/config/node-N.properties`.

| Layer | File | Variables | Purpose |
|-------|------|-----------|---------|
| 1 | `config/defaults.properties` | S3 vars | S3 endpoints, KRaft basics |
| 2 | `config/role/server.properties` or `broker.properties` | S3 vars, `NODE_ID` | Listeners, process roles |
| 3 | `config/role/node.properties` | `NODE_ID`, `CLUSTER_ID`, `VOTERS`, `BOOTSTRAP` | KRaft identity |
| 4 | `config/features/*.properties` | All vars + `AZ_NAME` | Feature-specific settings |
| 5 | `config/custom.properties` | verbatim (no substitution) | Highest priority overrides |

### Variables

**S3 variables** (defined in `justfile`, Layer 1/2/4):

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_ENDPOINT` | `http://minio:9000` | S3 endpoint URL |
| `S3_DATA_BUCKET` | `automq-data` | Data bucket |
| `S3_OPS_BUCKET` | `automq-ops` | Ops bucket |
| `S3_PATH_STYLE` | `true` | Path-style access (required for MinIO) |

**Per-node variables** (computed per node, Layer 2/3/4):

| Variable | Example | Description |
|----------|---------|-------------|
| `NODE_ID` | `0`, `1`, `2` | Node index (0-based) |
| `CLUSTER_ID` | `abc-123` | Stable UUID, persisted in `.devkit/cluster-id` |
| `VOTERS` | `0@node-0:9093,1@node-1:9093` | KRaft controller voter list |
| `BOOTSTRAP` | `node-0:9093,node-1:9093` | Controller bootstrap servers |
| `AZ_NAME` | `az-0`, `az-1`, `az-2` | Round-robin by `NODE_ID % 3` (used by zerozone) |

### Customization examples

**Use real AWS S3** — edit `justfile`:
```just
S3_REGION      := "ap-northeast-1"
S3_ENDPOINT    := "https://s3.ap-northeast-1.amazonaws.com"
S3_DATA_BUCKET := "my-automq-data"
S3_OPS_BUCKET  := "my-automq-ops"
S3_PATH_STYLE  := "false"
```

**Change listeners (e.g. SASL)** — edit `config/role/server.properties`:
```properties
process.roles=broker,controller
listeners=SASL_PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=SASL_PLAINTEXT://node-${NODE_ID}:9092,CONTROLLER://node-${NODE_ID}:9093
```

**Add broker settings** — create `config/custom.properties` (gitignored):
```properties
num.partitions=4
log.retention.hours=24
```

**Add a new feature** — create `config/features/my-feature.properties`:
```properties
my.setting=value
my.s3.path=s3://${S3_DATA_BUCKET}/my-prefix
my.node.label=node-${NODE_ID}
```
```bash
just start 1 my-feature
```
