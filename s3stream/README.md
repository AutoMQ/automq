## S3Stream: A Shared Streaming Storage Library
S3Stream is a shared streaming storage library that provides a unified interface for reading and writing streaming data to cloud object storage services like Amazon S3, Google Cloud Storage, and Azure Blob Storage. EBS is utilized here for its low-latency capabilities. It is designed to be used as the storage layer for distributed systems like Apache Kafka, Apache RocketMQ, etc. It provides the following features:
* **High Reliability**: S3Stream leverages cloud storage services(EBS and S3) to achieve zero RPO, RTO in seconds and 99.999999999% durability.
* **Cost Effective**: S3Stream is designed for optimal cost and efficiency on the cloud. It can cut Apache Kafka billing by 90% on the cloud.
* **Unified Interface**: S3Stream provides a unified interface for reading and writing streaming data to cloud object storage services.
* **High Performance**: S3Stream is optimized for high performance and low latency. It can handle high throughput and low latency workloads.
* **Scalable**: S3Stream is designed to be scalable and can handle large volumes of data. It can scale horizontally to handle increasing workloads.
* **Fault Tolerant**: S3Stream is fault tolerant and can recover from failures.

## S3Stream APIs
S3Stream provides a set of APIs for reading and writing streaming data to cloud object storage services. The APIs are designed to be simple and easy to use. The following APIs are provided by S3Stream:
```java
public interface Stream {
    /**
     * Get stream id
     */
    long streamId();

    /**
     * Get stream start offset.
     */
    long startOffset();

    /**
     * Get stream next append record offset.
     */
    long nextOffset();

    /**
     * Append RecordBatch to stream.
     */
    CompletableFuture<AppendResult> append(RecordBatch recordBatch);

    /**
     * Fetch RecordBatch list from a stream. 
     */
    CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint);

    /**
     * Trim stream.
     */
    CompletableFuture<Void> trim(long newStartOffset);
}
```
> Please refer to the [S3Stream API](src/main/java/com/automq/stream/api/Stream.java) for the newest API details.

## S3Stream Architecture
![image](../docs/images/automq_s3stream_architecture.gif)

In S3Stream's core architecture, data is initially written to the Write-Ahead Log (WAL) persistently, then it's uploaded to S3 storage in a near real-time fashion. To efficiently support two reading paradigms—Tailing Read and Catch-up Read—S3Stream incorporates a built-in Message Cache to expedite reading operations.
- **WAL Storage**: Opt for a storage medium with low latency; each WAL disk requires only a few GiB of space, with cloud storage like EBS typically being the choice.
- **S3 Storage**: Select the cloud provider's largest object storage service to offer high-throughput, cost-effective primary data storage solutions.
- **Message Cache**: Hot data and prefetched cold data are both stored in the cache to expedite reading. Simultaneously, they are efficiently evicted based on the consumer focus mechanism, thereby enhancing memory utilization efficiency.

## Various WAL Storage Options
![image](../docs/images/automq_wal_architecture.gif)
S3Stream supports various WAL storage options, including EBS, Regional EBS, S3, and other cloud storage services.
- **EBS WAL**: EBS is the default choice for WAL storage, offering low latency and high durability.
- **Regional EBS WAL**: On Azure, GCP, and Alibaba Cloud, Regional EBS replicas span multiple AZs.
- **S3 WAL**: Utilizing S3 as a WAL eliminates the need for EBS, streamlining the architecture to be fully S3-based, thus simplifying operations and maintenance. If your current setup is limited to MinIO, this is an excellent option.
- **S3 Express WAL**: AWS provides S3 Express, a high-performance, low-latency object storage solution that is well-suited as a storage choice for the S3Stream WAL.

### Object WAL prefix sharding

Both `s3.wal.path` and each bucket URI in `automq.zonerouter.channels` accept:

| URI option | Default | Behavior |
| --- | --- | --- |
| `shardCount` | `64` | Positive number of deterministic object-key shards. |
| `shardMode` | `rw` | `rw` writes sharded keys and reads sharded keys first; `r` writes legacy keys and reads legacy keys first. Both fall back to the other layout on object-not-found. |

The shard is `(objectStartOffset / 67108864) % shardCount`, encoded as lowercase
hexadecimal with at least two digits. It is inserted before the broker epoch:

```text
<MD5(1001)>/_kafka_clusterA/1001_rc/s00/42/wal/0-67108864
<MD5(1001)>/_kafka_clusterA/1001_rc/s01/42/wal/67108864-134217728
```

Traditional object WAL uses the same layout without `_rc` on the broker directory.
Broker-prefix listing discovers both layouts, and recovery and trimming use the
stored object paths. Single and batch reads prefer the configured write layout and
fall back to the other layout only for an object-not-found response. No offsets or router
protocol fields change, and existing objects do not need to be copied.

New clusters formatted with the default release version finalize `automq.version=7`
(AutoMQ V6) and use sharded writes by default. Existing clusters at older feature
levels must explicitly set `shardMode=r` on `s3.wal.path` and every writable router
bucket before upgrading. Otherwise broker startup fails before WAL initialization,
with an error identifying the required migration setting. Read-only router buckets
and local WALs are exempt because they do not write new sharded objects. Recovery
and failover may reopen older stored WAL URIs without `shardMode`; their writers
keep legacy keys until the cluster has finalized sharding support.

For a rolling upgrade:

1. Configure `shardMode=r` with the intended `shardCount` on all object WAL writers.
2. Upgrade all brokers and controllers, retaining `r` throughout the rollout.
3. Finalize reader support after the upgrade:

   ```sh
   bin/kafka-features.sh --bootstrap-server <broker>:9092 upgrade --feature automq.version=7
   ```

   Feature validation rejects this update while registered nodes do not support it.
   Older brokers cannot rejoin once the new feature level is finalized.
4. Remove `shardMode=r` or explicitly set `shardMode=rw`, then restart the writers.

For example, these configurations enable sharded writes on a compatible cluster:

```properties
s3.wal.path=0@s3://wal-bucket?region=us-west-2&shardCount=64&shardMode=rw
automq.zonerouter.channels=0@s3://router-bucket?region=us-west-2&mode=rw&shardCount=64&shardMode=rw
```

Router bucket `mode=r|rw` controls whether a bucket receives writes and is
independent of `shardMode`. Configure the same shard count for a given WAL bucket
on all brokers that read or write it, including read-only router buckets. Keep the
count unchanged while sharded objects are retained: offsets do not encode it.
To change the count, switch all writers to `shardMode=r`, wait for the old sharded
objects to be trimmed, configure the new count on all readers, and then enable
`rw` again. Switching back to `r` does not remove the cluster capability requirement:
feature downgrades below `automq.version=7`, including unsafe downgrades, are rejected
because the controller cannot verify that retained sharded objects have been drained.
After finalizing the feature, binary rollback must retain sharded-WAL reader support.

Sharding spreads successive objects across key prefixes to reduce concentrated
S3 request traffic. Validate the count against request rates and `503 SlowDown`
responses; directory names alone do not guarantee S3 partition throughput.
