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

# Replaying sampled records with automq-perf

Sample replay reuses the performance tool's producer clients, reporting,
warmup/test duration and shutdown. It is a produce-only mode (`-g 0`).
Broker code and normal synthetic payload generation are unchanged.

## Binary samples

`--sample-file` accepts a raw or gzip-compressed stream of records:

```text
int32 little-endian key_length
key_length bytes of key
int32 little-endian value_length
value_length bytes of value
```

Lengths are in bytes. `-1` represents null (no following bytes), and `0`
represents an empty field. Each occurrence is retained, including duplicate
keys/values. Keys are serialized as bytes, without text/base64 conversion.
Headers, source topic, partition, timestamps and producer identities are not
part of this binary format. The destination topic comes from the normal
performance-tool topic options.

The complete corpus is loaded once before Kafka clients are created. The
`--sample-max-bytes` guard defaults to 512 MiB and charges key/value bytes plus
64 bytes per record. There is also a one-million-record limit. This bounds
input allocations, not the JVM's total heap: leave room for descriptors,
timing assignments and Kafka producer buffers. Gzip is decompressed at load
time, not in the send loop. Invalid lengths, truncated files and empty samples
fail before any topic operations.

For example, create a small, entirely synthetic fixture:

```python
import gzip
import struct

records = [(b"key-a", b"value-1"), (b"key-b", b"value-2"), (b"key-a", b"value-3")]
with gzip.open("records.gz", "wb") as out:
    for key, value in records:
        for field in (key, value):
            out.write(struct.pack("<i", len(field)))
            out.write(field)
```

Replay at a configured aggregate record rate:

```sh
bin/automq-perf-test.sh -B localhost:9092 -X replay -t 1 -n 32 -g 0 -p 2 \
  --sample-file records.gz --send-rate 10000 --omit-send-time-header \
  -P compression.type=none linger.ms=200 batch.size=65536
```

Records are assigned by file ordinal to the configured producers, then each
producer cycles through its assigned records in order. This is a synthetic
producer assignment; it does not recover the original sender. Multiple topics
each receive a copy of the corpus, sharing the configured aggregate rate.
For variable-size samples, use `--send-rate`: `--send-throughput` would otherwise
calculate a rate using the synthetic `--record-size`. Reported payload bytes
exclude keys, headers and Kafka framing, so measure broker ingress separately.

Sample mode defaults to `partitioner.ignore.keys=false`. Synthetic mode retains
its default of `true`; explicit producer settings are honored in both modes.
Remove an explicit round-robin/custom partitioner to use Kafka's built-in keyed
partitioning. Destination partition count affects the hash mapping; replaying
onto a different partition count does not preserve source partition IDs.

`--values-file` remains the existing Avro input mode and cannot be combined
with `--sample-file`. Sample replay does not use synthetic size/randomness
settings and cannot be combined with backlog or catch-up modes.

## Optional timing

A separate `--sample-timing-file` supplies the missing timing and producer
assignment. It is UTF-8 CSV with the exact header `producer,offset_ns` and one
row per binary record, in the same order. Producer IDs are local, zero-based
indices below `--producers-per-topic`. Each producer must have at least one
record. Offsets must be nonnegative and nondecreasing within each producer;
equal offsets represent a burst. The combined file need not be time sorted.

For the three-record fixture above:

```csv
producer,offset_ns
0,100000000
1,0
0,110000000
```

With a 1000 ms cycle, producer 0 sends at 100 ms and 110 ms; producer 1 sends
at 0 ms. The next cycle starts at 1000 ms. The explicit cycle length preserves
the final idle gap and must be strictly greater than every offset.

```sh
bin/automq-perf-test.sh -B localhost:9092 -X replay -t 1 -n 32 -g 0 -p 2 \
  --sample-file records.gz --sample-timing-file timing.csv \
  --sample-cycle-ms 1000 --sample-speed 1 --omit-send-time-header \
  -P compression.type=none linger.ms=200 batch.size=65536
```

`--sample-speed 1` retains supplied gaps; `2` halves gaps and cycle length.
Timed mode rejects record-rate/throughput flags. Warmup repeats the same timed
pattern without a rate ramp. Each producer has its own dispatch thread and
schedule so a blocked producer does not block other producers. Within each
process, all schedules use a shared monotonic start.

For a fleet, split records and matching timing rows into disjoint files per
process, renumber producer IDs locally, and retain offsets relative to the
same capture origin and the same cycle length. Set the same future
`--sample-start-time-ms` (Unix epoch milliseconds) on every process to coordinate
the start. Topic/client preparation must finish before that time; a late
process fails instead of compressing the missed period into a burst. The
measurement clock starts after this wait. Cross-host clock synchronization
and OS scheduling still limit timing precision. Do not run every process
against the complete corpus unless duplication is intentional.

Timed replay is best effort, not a real-time scheduler. Late dispatch shifts
that producer's subsequent schedule by the delay, retaining its remaining
gaps rather than aggressively draining overdue events. Total schedule stretch,
summed across producers, is logged on shutdown. A buffer-limited producer can
therefore achieve less than the requested rate. ACK failures remain part of
the existing error statistics; background replay failures terminate reporting
with an error.

A key/value sample alone cannot recreate original timing or batches. Consumer
poll times contain fetch batching and lag; they are not producer send times.
Kafka record timestamps can be application timestamps or broker append times,
and may have only millisecond precision. For a producer-path reproduction,
record relative enqueue times and logical producer identity at the source,
using a shared capture origin. A supplied approximate/synthetic schedule is
useful for experiments but must be identified as such. Samples collected with
equal per-partition quotas can also distort original traffic weights.

Actual RecordBatches and Produce RPCs are rebuilt by the Kafka producer.
Validate records per batch, batch sizes, partition appends/s, RPC/s, active
partitions and achieved bytes/s against the reference. Producer count, linger,
buffering, compression and backpressure all affect these results. A Java
`batch.size` is an allocation size; do not blindly copy another client's large
maximum batch limit across thousands of active partitions. With uncompressed
original payloads, matching compressed bytes/s will generally reduce records/s.

## Optional benchmark header

By default automq-perf adds the `st` send-time header, as before.
`--omit-send-time-header` omits it in either synthetic or sample mode and
requires `--groups-per-topic 0`, since the benchmark consumer uses this header
for latency accounting. Producer ACK latency is still measured locally.
This option does not alter replay scheduling or Kafka's record timestamp.
Original sample headers are not imported by this format.
