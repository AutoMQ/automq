# AutoMQ Log Uploader Module

该模块提供基于 Log4j 1.x 的 S3 日志异步上传能力，其他子模块只需依赖此模块并按照简单配置即可把日志同步写入对象存储。核心组件：

- `com.automq.log.uploader.S3RollingFileAppender`：继承 `RollingFileAppender`，在写本地文件的同时将日志事件推送给上传器。
- `com.automq.log.uploader.LogUploader`：异步缓冲、压缩并上传日志；支持按配置开关和定期清理。
- `com.automq.log.uploader.S3LogConfig`/`S3LogConfigProvider`：抽象上传所需配置，默认实现 `PropertiesS3LogConfigProvider` 会读取 `automq-log.properties`。

## 快速集成

1. 在模块 `build.gradle` 中添加依赖：
   ```groovy
   implementation project(':automq-log-uploader')
   ```
2. 在资源目录创建 `automq-log.properties`（或者自定义 `S3LogConfigProvider`）：
   ```properties
   log.s3.enable=true
   log.s3.bucket=0@s3://your-log-bucket?region=us-east-1
   log.s3.cluster.id=my-cluster
   log.s3.node.id=1
   log.s3.selector.type=kafka
   log.s3.selector.kafka.bootstrap.servers=PLAINTEXT://kafka:9092
   log.s3.selector.kafka.group.id=automq-log-uploader-my-cluster
   ```
3. 在 `log4j.properties` 中引用 Appender：
   ```properties
   log4j.appender.s3_uploader=com.automq.log.uploader.S3RollingFileAppender
   log4j.appender.s3_uploader.File=logs/server.log
   log4j.appender.s3_uploader.MaxFileSize=100MB
   log4j.appender.s3_uploader.MaxBackupIndex=10
   log4j.appender.s3_uploader.layout=org.apache.log4j.PatternLayout
   log4j.appender.s3_uploader.layout.ConversionPattern=[%d] %p %m (%c)%n
   ```
   如需自定义配置提供器，可额外设置：
   ```properties
  log4j.appender.s3_uploader.configProviderClass=com.example.CustomS3LogConfigProvider
  ```

## 关键配置说明

| 配置项 | 说明 |
| ------ | ---- |
| `log.s3.enable` | 是否启用 S3 上传功能。
| `log.s3.bucket` | 推荐使用 AutoMQ Bucket URI（如 `0@s3://bucket?region=us-east-1&pathStyle=true`）。若为简写桶名，需要额外提供 `log.s3.region` 等字段。
| `log.s3.cluster.id` / `log.s3.node.id` | 用于构造对象存储路径 `automq/logs/{cluster}/{node}/{hour}/{uuid}`。
| `log.s3.selector.type` | 选主策略（`static`、`nodeid`、`file`、`kafka` 或自定义）。
| `log.s3.primary.node` | 搭配 `static` 策略使用，指示当前节点是否为主节点。
| `log.s3.selector.kafka.*` | Kafka 选主所需的附加配置，如 `bootstrap.servers`、`group.id` 等。
| `log.s3.active.controller` | **已废弃**，请改用 `log.s3.selector.type=static` + `log.s3.primary.node=true`。

上传调度可通过环境变量覆盖：

- `AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL`：最大上传间隔（毫秒）。
- `AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL`：保留时长（毫秒），旧对象早于该时间会被清理。

### 选主策略

为避免多个节点同时执行 S3 清理任务，日志上传器内置与 OpenTelemetry 模块一致的选主机制：

1. **static**：通过 `log.s3.primary.node=true|false` 指定哪个节点为主。
2. **nodeid**：当 `log.s3.node.id` 等于 `primaryNodeId` 时成为主节点，可在 URL 或属性中设置 `log.s3.selector.primary.node.id`。
3. **file**：使用共享文件做抢占式选主，配置 `log.s3.selector.file.leaderFile=/shared/leader`、`log.s3.selector.file.leaderTimeoutMs=60000`。
4. **kafka**：默认策略。所有节点加入单分区主题的同一消费组，持有分区的节点成为主节点。必要配置：
   ```properties
   log.s3.selector.type=kafka
   log.s3.selector.kafka.bootstrap.servers=PLAINTEXT://kafka:9092
   log.s3.selector.kafka.topic=__automq_log_uploader_leader_cluster1
   log.s3.selector.kafka.group.id=automq-log-uploader-cluster1
   ```
   可通过 `log.s3.selector.kafka.*` 提供安全（SASL/SSL）、超时等高级参数。
5. **自定义**：实现 `com.automq.log.uploader.selector.LogUploaderNodeSelectorProvider` 并通过 SPI 注册，即可引入自定义选主策略。

## 扩展

如果应用已有自己的依赖注入/配置方式，可实现 `S3LogConfigProvider` 并在启动时调用：

```java
import com.automq.log.uploader.S3RollingFileAppender;

S3RollingFileAppender.setConfigProvider(new CustomConfigProvider());
```

所有 `S3RollingFileAppender` 实例会共用这个 provider。
