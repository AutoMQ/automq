## S3Stream: A Shared Streaming Storage Library
S3Stream is a shared streaming storage library offering a unified interface for reading and writing streaming data to cloud object storage services such as Amazon S3, Google Cloud Storage, Azure Blob Storage, and any S3-compatible storage like MinIO. It is designed to be used as the storage layer for distributed streaming storage systems like Apache Kafka. It provides the following features:
* **High Reliability**: S3Stream leverages cloud storage services to achieve zero RPO, RTO in seconds and 99.999999999% durability.
* **Cost Effective**: S3Stream is designed for optimal cost and efficiency on the cloud. It can cut Apache Kafka billing by 90% on the cloud.
* **Unified Interface**: S3Stream provides a unified interface for reading and writing streaming data to cloud object storage services.
* **High Performance**: S3Stream is optimized for high performance and low latency. It can handle high throughput and low latency workloads.
* **Scalable**: S3Stream is designed to be scalable and can handle large volumes of data. It can scale horizontally to handle increasing workloads.
* **Fault Tolerant**: S3Stream is fault tolerant and can recover from failures.

## S3 client selection

Starting with AutoMQ 1.8, S3 bucket URIs use the AWS CRT-based S3 client by default.
CRT manages connection pooling, DNS load balancing, and failed-address recovery inside the SDK.
AutoMQ continues to manage multipart uploads and precompute request checksums.

To use the Java-based S3 client with Netty instead, add `sdkImpl=netty` to each affected bucket URI:

```properties
s3.data.buckets=0@s3://data-bucket?region=us-east-1&sdkImpl=netty
s3.ops.buckets=1@s3://ops-bucket?region=us-east-1&sdkImpl=netty
```

Client selection applies per bucket, including S3-backed WAL buckets. Restart the process after changing a bucket URI.
On AutoMQ 1.7 releases that include CRT support, select CRT explicitly with `sdkImpl=crt`.

Consider these differences when upgrading:

* `apiCallTimeoutMs` and `apiCallAttemptTimeoutMs` apply only to Netty. CRT uses `connectionTimeoutMs`
  and connection health checks, which do not provide an equivalent deadline for an entire API call.
  The default health threshold is 32 MiB/s per connection over two seconds; slower endpoints may need
  `crtMinimumThroughputInBps` and `crtMinimumThroughputTimeoutSeconds` adjusted.
* CRT uses native memory outside the JVM heap. `crtMaxNativeMemoryLimitInBytes` can set a per-client
  memory budget; otherwise CRT calculates one. Multiple buckets and isolated read/write clients create
  separate client budgets.
* CRT uses native networking and TLS. Validate S3-compatible endpoints, proxy settings, private certificate
  authorities, and native library loading in the deployment environment before upgrading. Keep
  `sdkImpl=netty` for deployments that require the existing Java TLS or proxy behavior.

See the [AWS CRT S3 client documentation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html)
for SDK configuration differences.

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
