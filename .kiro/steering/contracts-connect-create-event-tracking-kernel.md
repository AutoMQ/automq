# Connect Create Event Tracking Kernel Contracts

## Input Artifacts

Archaeology source: `.kiro/steering/archaeology-connect-create-event-tracking-kernel.md`.

## Contract List

| # | Contract | Decision | Verification |
| --- | --- | --- | --- |
| K-C1 | Kernel startup failure signal is additive and log-based. | Emit one stable structured line on fatal startup failure; do not change process exit behavior. | Unit/log assertion around `AbstractConnectCli` catch path where feasible. |
| K-C2 | Structured log schema is stable and simple. | Prefix `AUTOMQ_CONNECT_STARTUP_FAILURE` followed by JSON with `stage`, `errorClass`, `message`. | CMP parser test parses JSON; missing/bad JSON falls back to plain logs. |
| K-C3 | Plugin download failure is not kernel-owned. | Init container download errors are diagnosed from K8s init container status/logs in CMP. | No kernel changes for init container. |
| K-C4 | JVM plugin load/startup failure can be kernel-owned only when Worker process reaches JVM and fails. | `AbstractConnectCli` catch blocks emit structured log for top-level startup and `connect.start()` failure. | Force a startup exception test if existing test hooks allow. |
| K-C5 | Offset modification semantics are not changed. | CMP must adapt to STOPPED/taskCount rules; kernel code remains behavior-compatible. | Existing `DistributedHerderTest` / `StandaloneHerderTest` offset tests continue passing. |

## Interaction Answers

### CMP log diagnosis -> Kernel startup logs

| # | Question | Answer |
| --- | --- | --- |
| Q1 Trigger | Worker container terminates/fails or CMP REST verification times out after pod readiness. |
| Q2 Normal path | No structured failure line is emitted; CMP REST/plugin checks succeed. |
| Q3 Failure path | Kernel emits `AUTOMQ_CONNECT_STARTUP_FAILURE {...}` once at the fatal catch site; CMP includes parsed details in event error message. |
| Q4 Consistency | Kernel log is diagnostic only; CMP remains source of truth for task/change state. |
| Q5 Timing | CMP reads logs only after K8s status suggests failure or after timeout; it does not stream logs as state. |

### CMP initial offsets -> Kafka Connect framework

| # | Question | Answer |
| --- | --- | --- |
| Q1 Trigger | Connector create includes initial offsets. |
| Q2 Normal path | CMP calls `/stop`, waits STOPPED/no RUNNING tasks via `/status`, then PATCH offsets, then `/resume`. |
| Q3 Failure path | Existing framework BadRequest remains unchanged; CMP task handles it as failed/retry according to contract. |
| Q4 Consistency | Kernel remains authoritative for accepting/rejecting offsets. |
| Q5 Timing | No kernel timing changes. |

## Atomic Execution Gate

Kernel changes are limited to startup structured log helpers/catch-site calls unless implementation discovers a missing compile dependency. No Connect REST offset behavior should be modified.
