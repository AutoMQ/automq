# AutoMQ Log Uploader Module

This module provides asynchronous S3 log upload capability based on Log4j 1.x. Other submodules only need to depend on this module and configure it simply to synchronize logs to object storage. Core components:

- `com.automq.log.S3RollingFileAppender`: Extends `RollingFileAppender`, pushes log events to the uploader while writing to local files.
- `com.automq.log.uploader.LogUploader`: Asynchronously buffers, compresses, and uploads logs; supports configuration switches and periodic cleanup.
- `com.automq.log.uploader.S3LogConfig`: Interface that abstracts the configuration required for uploading. Implementations must provide cluster ID, node ID, object storage instance, and leadership status.

## Quick Integration

1. Add dependency in your module's `build.gradle`:
   ```groovy
   implementation project(':automq-log-uploader')
   ```
2. Implement or provide an `S3LogConfig` instance and configure the appender:

   ```java
   // Set up the S3LogConfig through your application
   S3LogConfig config = // your S3LogConfig implementation
   S3RollingFileAppender.setup(config);
   ```
3. Reference the Appender in `log4j.properties`:

   ```properties
   log4j.appender.s3_uploader=com.automq.log.S3RollingFileAppender
   log4j.appender.s3_uploader.File=logs/server.log
   log4j.appender.s3_uploader.MaxFileSize=100MB
   log4j.appender.s3_uploader.MaxBackupIndex=10
   log4j.appender.s3_uploader.layout=org.apache.log4j.PatternLayout
   log4j.appender.s3_uploader.layout.ConversionPattern=[%d] %p %m (%c)%n
   ```

## S3LogConfig Interface

The `S3LogConfig` interface provides the configuration needed for log uploading:

```java
public interface S3LogConfig {
    boolean isEnabled();           // Whether S3 upload is enabled
    String clusterId();            // Cluster identifier  
    int nodeId();                  // Node identifier
    ObjectStorage objectStorage(); // S3 object storage instance
    boolean isLeader();            // Whether this node should upload logs
}
```


The upload schedule can be overridden by environment variables:

- `AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL`: Maximum upload interval (milliseconds).
- `AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL`: Retention period (milliseconds), old objects earlier than this time will be cleaned up.

## Implementation Notes

### Leader Selection

The log uploader relies on the `S3LogConfig.isLeader()` method to determine whether the current node should upload logs and perform cleanup tasks. This avoids multiple nodes in a cluster simultaneously executing these operations.

### Object Storage Path

Logs are uploaded to object storage following this path pattern:
```
automq/logs/{clusterId}/{nodeId}/{hour}/{uuid}
```

Where:
- `clusterId` and `nodeId` come from the S3LogConfig
- `hour` is the timestamp hour for log organization  
- `uuid` is a unique identifier for each log batch

## Usage Example

Complete example of using the log uploader:

```java
import com.automq.log.S3RollingFileAppender;
import com.automq.log.uploader.S3LogConfig;
import com.automq.stream.s3.operator.ObjectStorage;

// Implement S3LogConfig
public class MyS3LogConfig implements S3LogConfig {
    @Override
    public boolean isEnabled() {
        return true; // Enable S3 upload
    }
    
    @Override
    public String clusterId() {
        return "my-cluster";
    }
    
    @Override
    public int nodeId() {
        return 1;
    }
    
    @Override
    public ObjectStorage objectStorage() {
        // Return your ObjectStorage instance
        return myObjectStorage;
    }
    
    @Override
    public boolean isLeader() {
        // Return true if this node should upload logs
        return isCurrentNodeLeader();
    }
}

// Setup and use
S3LogConfig config = new MyS3LogConfig();
S3RollingFileAppender.setup(config);

// Configure Log4j to use the appender
// The appender will now automatically upload logs to S3
```

## Lifecycle Management

Remember to properly shutdown the log uploader when your application terminates:

```java
// During application shutdown
S3RollingFileAppender.shutdown();
```
