<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# S3 request attribution

GET and PUT latency metrics can identify the WAL that initiated a request.
RouterWAL GETs can also identify why the payload was read. Both dimensions
are disabled by default to control metric cardinality.

Set these JVM system properties before starting the broker:

| Property | Default | Effect |
| --- | --- | --- |
| `automq.s3.metrics.source.enabled` | `false` | Adds `source` to GET and PUT metrics. |
| `automq.s3.metrics.read.purpose.enabled` | `false` | With source attribution enabled, adds `read_purpose` to RouterWAL GET metrics. |

For both dimensions, add the following to the broker JVM options:

```sh
-Dautomq.s3.metrics.source.enabled=true
-Dautomq.s3.metrics.read.purpose.enabled=true
```

These are JVM properties, not `server.properties` entries. They are read once
when object-storage metrics initialize; changing them requires a process restart.
Existing deployments that use the `source` label must explicitly enable the
first property after upgrading.

| Source flag | Purpose flag | GET and PUT labels |
| --- | --- | --- |
| `false` | Either | No `source` or `read_purpose`. Requests share the original histograms by operation, status and size. |
| `true` | `false` | `source` only. |
| `true` | `true` | `source`, plus `read_purpose` on GETs whose source is `ROUTER_WAL`. |

The dimensions apply to all five exports of `kafka_stream_operation_latency`:
`_count`, `_sum`, `_50p`, `_99p`, and `_max`. Count and sum remain cumulative.
Each observation is recorded once. Disabled dimensions are collapsed before
histogram registration, so removing a label does not create duplicate series
or lose observations. Existing size bands and status labels remain intact.
LIST, DELETE and multipart metrics retain their current labels and behavior.

## Sources

| `source` | Meaning |
| --- | --- |
| `ROUTER_WAL` | RouterWAL payload persistence or reads. |
| `DELTA_WAL` | Delta WAL persistence or reads, including reservation verification when configured. |
| `UNCLASSIFIED` | Calls without a WAL source annotation, including data-bucket work. |
| `MIXED` | One physical GET merged from callers with different sources. |

Source identifies WAL ownership, not an exclusive phase of a producer request.
Shared WAL helpers can also issue background or recovery requests under the
same source. Compaction categories are not introduced here.

## RouterWAL GET purposes

| `read_purpose` | Meaning |
| --- | --- |
| `ROUTER_PRODUCE` | The remote target reads a routed producer payload before appending it. |
| `SNAPSHOT_REPLAY` | `SnapshotReadCache` decodes a linked payload while applying a WAL snapshot to its in-memory read cache. |
| `OTHER` | An unannotated caller, including recovery reads. |
| `MIXED` | One physical RouterWAL GET merged from callers with different purposes. |

`SNAPSHOT_REPLAY` describes background snapshot processing, not a direct Kafka
consumer Fetch request. Purpose follows the asynchronous read through the router
channel, WAL reader, object-layout fallback and retries. Existing callers can
continue to use the original read methods; they default to `OTHER`.

Attribution never prevents compatible reads from merging. A combined GET
counts once as `MIXED` rather than being counted once for each caller. A GET
whose source is itself `MIXED` has no RouterWAL-only purpose label.

These metrics keep their existing storage-attempt observation boundaries.
Retries and hedged completions can produce more than one observation for a
logical read; cache hits that avoid object storage produce none. Attribution
does not change the cache, merge interval, retry policy or durability behavior.
