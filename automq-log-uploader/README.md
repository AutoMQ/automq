# AutoMQ Log Uploader Module

This module provides asynchronous S3 log upload capability based on Log4j 1.x. Other submodules only need to depend on this module and configure it simply to synchronize logs to object storage. Core components:

- `com.automq.log.uploader.S3RollingFileAppender`: Extends `RollingFileAppender`, pushes log events to the uploader while writing to local files.
- `com.automq.log.uploader.LogUploader`: Asynchronously buffers, compresses, and uploads logs; supports configuration switches and periodic cleanup.
- `com.automq.log.uploader.S3LogConfig`/`S3LogConfigProvider`: Abstracts the configuration required for uploading. The default implementation `PropertiesS3LogConfigProvider` reads from `automq-log.properties`.

## Quick Integration

1. Add dependency in your module's `build.gradle`:
   ```groovy
   implementation project(':automq-log-uploader')
   ```
2. Create `automq-log.properties` in the resources directory (or customize `S3LogConfigProvider`):
   ```properties
   log.s3.enable=true
   log.s3.bucket=0@s3://your-log-bucket?region=us-east-1
   log.s3.cluster.id=my-cluster
   log.s3.node.id=1
   log.s3.selector.type=kafka
   log.s3.selector.kafka.bootstrap.servers=PLAINTEXT://kafka:9092
   log.s3.selector.kafka.group.id=automq-log-uploader-my-cluster
   ```
3. Reference the Appender in `log4j.properties`:
   ```properties
   log4j.appender.s3_uploader=com.automq.log.uploader.S3RollingFileAppender
   log4j.appender.s3_uploader.File=logs/server.log
   log4j.appender.s3_uploader.MaxFileSize=100MB
   log4j.appender.s3_uploader.MaxBackupIndex=10
   log4j.appender.s3_uploader.layout=org.apache.log4j.PatternLayout
   log4j.appender.s3_uploader.layout.ConversionPattern=[%d] %p %m (%c)%n
   ```
   If you need to customize the configuration provider, you can set:
   ```properties
  log4j.appender.s3_uploader.configProviderClass=com.example.CustomS3LogConfigProvider
  ```

## Key Configuration Description

| Configuration Item | Description |
| ------ | ---- |
| `log.s3.enable` | Whether to enable S3 upload function.
| `log.s3.bucket` | It is recommended to use AutoMQ Bucket URI (e.g. `0@s3://bucket?region=us-east-1&pathStyle=true`). If using a shorthand bucket name, additional fields such as `log.s3.region` need to be provided.
| `log.s3.cluster.id` / `log.s3.node.id` | Used to construct the object storage path `automq/logs/{cluster}/{node}/{hour}/{uuid}`.
| `log.s3.selector.type` | Leader election strategy (`static`, `nodeid`, `file`, `kafka`, `controller`, `connect-leader`, or custom).
| `log.s3.primary.node` | Used with `static` strategy to indicate whether the current node is the primary node.
| `log.s3.selector.kafka.*` | Additional configuration required for Kafka leader election, such as `bootstrap.servers`, `group.id`, etc.

The upload schedule can be overridden by environment variables:

- `AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL`: Maximum upload interval (milliseconds).
- `AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL`: Retention period (milliseconds), old objects earlier than this time will be cleaned up.

### Leader Election Strategies

To avoid multiple nodes executing S3 cleanup tasks simultaneously, the log uploader has a built-in leader election mechanism consistent with the OpenTelemetry module:

1. **static**: Specify which node is the leader using `log.s3.primary.node=true|false`.
2. **nodeid**: Becomes the leader node when `log.s3.node.id` equals `primaryNodeId`, which can be set in the URL or properties with `log.s3.selector.primary.node.id`.
3. **file**: Uses a shared file for preemptive leader election, configure `log.s3.selector.file.leaderFile=/shared/leader`, `log.s3.selector.file.leaderTimeoutMs=60000`.
4. **controller** *(default for brokers)*: Defers to the Kafka KRaft controller leadership that AutoMQ exposes at runtime. No additional configuration is required—the broker registers a supplier and the uploader continuously checks it.
5. **connect-leader** *(default for Kafka Connect clusters)*: Mirrors the distributed herder leader election. Works out of the box when running inside AutoMQ’s Connect runtime.
6. **kafka**: All nodes join the same consumer group of a single-partition topic, the node holding the partition becomes the leader. Necessary configuration:
   ```properties
   log.s3.selector.type=kafka
   log.s3.selector.kafka.bootstrap.servers=PLAINTEXT://kafka:9092
   log.s3.selector.kafka.topic=__automq_log_uploader_leader_cluster1
   log.s3.selector.kafka.group.id=automq-log-uploader-cluster1
   ```
   Advanced parameters such as security (SASL/SSL), timeout, etc. can be provided through `log.s3.selector.kafka.*`.
7. **custom**: Implement `com.automq.log.uploader.selector.LogUploaderNodeSelectorProvider` and register it through SPI to introduce a custom leader election strategy.

> **Note**
>
> Runtime-backed selectors (`controller`, `connect-leader`) no longer require the S3 uploader to be initialized after leadership registration. The selector re-evaluates the registry on every invocation, so once the hosting runtime publishes its leader supplier the uploader automatically adopts it.

## Extension

If the application already has its own dependency injection/configuration method, you can implement `S3LogConfigProvider` and call it at startup:

```java
import com.automq.log.uploader.S3RollingFileAppender;

S3RollingFileAppender.setConfigProvider(new CustomConfigProvider());
```

All `S3RollingFileAppender` instances will share this provider.
