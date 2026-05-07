# Connect Create Event Tracking Kernel Archaeology

## Scope

Kernel repository baseline: `/Users/keqing/IdeaProjects/automq-connect-events`, branch `codex/connect-create-event-tracking`, based on `origin/main`.

This document covers only kernel facts needed by CMP Connect create event tracking: Kafka Connect startup, plugin scanning/loading, worker readiness, and offset modification semantics.

## Module List

| Module | Files | Ownership | External interface | Boundary basis |
| --- | --- | --- | --- | --- |
| Connect CLI startup | `connect/runtime/src/main/java/org/apache/kafka/connect/cli/AbstractConnectCli.java`, `ConnectDistributed.java`, `ConnectStandalone.java` | Worker process lifecycle and startup logs | Process exit code, stdout/stderr/logs | CMP can only observe this through K8s pod/container state and logs. |
| Plugin discovery | `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/Plugins.java`, `PluginScanner.java`, related isolation classes | Plugin classpath scan result inside Worker JVM | `/connector-plugins`, logs | Plugin download is outside JVM if done by init container; plugin load is inside JVM. |
| Connect REST offsets API | `ConnectorsResource.java`, `AbstractHerder.java` | REST API request validation and callback wiring | `PATCH /connectors/{connector}/offsets`, `PUT /stop`, `PUT /resume` | CMP initial-offset flow depends on this framework behavior. |
| Distributed herder | `DistributedHerder.java` | Distributed Connect state and task config snapshot | Herder callbacks, config topic | Offset modifications require leader/config snapshot/STOPPED/taskCount 0. |
| Standalone herder | `StandaloneHerder.java` | Standalone Connect state | Herder callbacks | Same STOPPED/taskCount constraint. |

## Dependency Graph

| Caller | Callee | Interface | Scenario |
| --- | --- | --- | --- |
| `AbstractConnectCli.run()` | `startConnect(workerProps)` | direct method call | Process startup. |
| `startConnect()` | `Plugins` | constructor and `compareAndSwapWithDelegatingLoader()` | Scans plugin classes before worker starts. |
| `startConnect()` | `ConnectRestServer` | `initializeServer`, `advertisedUrl` | REST server availability comes after plugin scan and config creation. |
| `startConnect()` | `Connect` / Herder | `connect.start()` | Worker becomes usable after herder/rest start. |
| CMP `ConnectRestClient` | `ConnectorsResource` | REST endpoints | Submit connector, stop/resume, patch offsets, query status/plugins. |
| `ConnectorsResource` | `Herder` | `alterConnectorOffsets`, `resetConnectorOffsets` | Offset write/reset. |
| `AbstractHerder` | `DistributedHerder` / `StandaloneHerder` | `modifyConnectorOffsets` | Framework-specific offset checks. |
| Herder | `Worker` | `modifyConnectorOffsets` | Actual offset write. |

## Implicit Constraints

| # | Constraint | Source | Consequence | Code |
| --- | --- | --- | --- | --- |
| K1 | Worker plugin scan happens before REST server is usable. | Code | CMP cannot verify plugin load via REST until after pod/container is running and REST responds. | `AbstractConnectCli.startConnect()` lines 143-152. |
| K2 | Generic startup failures are logged as `Stopping due to error` or `Failed to start Connect`, then process exits. | Code | CMP needs log parsing or structured log additions for reliable user-facing failure details. | `AbstractConnectCli` lines 124-126 and 165-171. |
| K3 | AutoMQ-specific telemetry/log initialization is already in CLI startup. | History/code | New logs must not reintroduce noisy stack behavior fixed by AutoMQ. | `e634884ec0`, `91459b2a17`; `AbstractConnectCli` imports AutoMQ log/metrics/S3 hooks. |
| K4 | Plugin scanner may catch `LinkageError`, skip faulty implementations, and continue. | Code comments | A bad plugin may produce logs without full worker startup failure; REST plugin verification is still required. | `PluginScanner` comments around LinkageError handling. |
| K5 | `PATCH /connectors/{connector}/offsets` rejects empty offsets before reaching herder. | Code | CMP must not send empty offset body. | `ConnectorsResource` lines 374-384; `AbstractHerder` lines 1160-1165. |
| K6 | Offsets can be modified only when connector target state is STOPPED and taskCount is 0. | Code | CMP initial offsets must stop and poll status before patching. | `DistributedHerder` lines 1729-1738; `StandaloneHerder` lines 449-452. |
| K7 | Distributed mode additionally requires no rebalance, leader handling, and config snapshot freshness. | Code | CMP should treat transient leader/rebalance failures as retryable task failures where possible. | `DistributedHerder` lines 1710-1722. |
| K8 | Exactly-once source connectors perform zombie fencing before offset modification. | Code/tests | Offset task may take longer than a simple REST write. | `DistributedHerder` lines 1672-1688. |
| K9 | Connector STOPPED state is an upstream KIP-875 framework concept. | Git history | CMP should not model this as AutoMQ-specific policy. | KIP-875 commits in `StandaloneHerder`/`ConnectorsResource` history. |

## Tests / History Read

| Source | Relevant facts |
| --- | --- |
| `DistributedHerderTest` offset test search | Tests cover alter/reset offsets, stopped snapshots, zombie fencing, bad request when connector is not stopped. |
| `ConnectStandaloneTest` and `ConnectRestServerTest` existence | Startup/rest behavior has unit coverage; add targeted tests near any startup logging change. |
| `AbstractConnectCli` history `e634884ec0` | AutoMQ already changed startup logging to reduce stack printing for S3 log path. |
| `DistributedHerder` history KAFKA-16943/KAFKA-10816 | Worker startup and health/readiness behavior is upstream-sensitive. |

## Naming And Pattern Rules

| Concept | Pattern | Example |
| --- | --- | --- |
| AutoMQ Connect runtime package | `org.apache.kafka.connect.automq.<domain>` | `automq.log`, `automq.metrics`, `automq.s3` |
| Startup logs | SLF4J through `getLogger()` in `AbstractConnectCli` | `getLogger().info/warn/error(...)` |
| Structured log addition | Prefer a stable one-line prefix or JSON payload emitted once at catch site | To be locked in contract before implementation. |
| Tests | Keep under `connect/runtime/src/test/java/...` beside changed runtime class | `ConnectStandaloneTest`, `DistributedHerderTest` |

## Framework Behavior Rules

| Framework | Rule |
| --- | --- |
| Kafka Connect CLI | `startConnect()` throws before process reaches REST if plugin/config/rest initialization fails; `connect.start()` failures exit code 3. |
| Kafka Connect REST | REST resource validates request shape, then waits/forwards through `HerderRequestHandler`. |
| Distributed Herder | Offset modification must be processed by leader with fresh config snapshot; non-leader returns `NotLeaderException`. |
| Standalone Herder | Offset modification is synchronized and checks local config state. |
| Plugin scanning | Some plugin load errors can be logged and skipped instead of failing Worker; REST plugin list must be verified after startup. |

## Contract Questions For Next Phase

1. Structured startup failure schema and prefix for CMP parsing.
2. Whether kernel needs new log only in `AbstractConnectCli` catch blocks, or also around plugin scanner skipped-plugin cases.
3. How CMP distinguishes init-container plugin download failures from JVM plugin load failures:
   - init-container failure from K8s pod status/logs.
   - JVM plugin load/startup failure from Worker container status/logs and optional structured log.
   - plugin missing after startup from `/connector-plugins` verification.
