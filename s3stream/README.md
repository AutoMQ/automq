## S3Stream: A Shared Streaming Storage Library
S3Stream is a shared streaming storage library offering a unified interface for reading and writing streaming data to cloud object storage services such as Amazon S3, Google Cloud Storage, Azure Blob Storage, and any S3-compatible storage like MinIO. It is designed to be used as the storage layer for distributed streaming storage systems like Apache Kafka. It provides the following features:
* **High Reliability**: S3Stream leverages cloud storage services to achieve zero RPO, RTO in seconds and 99.999999999% durability.
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
