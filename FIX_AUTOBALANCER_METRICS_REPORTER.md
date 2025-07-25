# Fix for AutoBalancer Metrics Reporter Issues (#2697)

## Problem Description

The AutoBalancer metrics reporter was experiencing several critical issues after upgrading from version 1.1.2 to 1.4.1:

1. **OutOfOrderSequenceException**: "The broker received an out of order sequence number"
2. **Missing consumption data curves**: Metrics data gaps causing incomplete monitoring dashboards
3. **InterruptException during shutdown**: Improper handling of producer flush during shutdown
4. **Failed to send metrics warnings**: High failure rates in metric transmission

## Root Cause Analysis

### Primary Issue: Missing Producer Idempotence
The root cause was that the AutoBalancer metrics reporter was configured with:
- `retries = 5` 
- `acks = all`
- **BUT missing `enable.idempotence = true`**

Without idempotence, when the producer retries failed requests, it can send records with sequence numbers that appear out of order to the broker, causing `OutOfOrderSequenceException`.

### Secondary Issues
1. **Poor error handling**: Generic error logging without differentiating error types
2. **Improper shutdown**: Not gracefully handling producer flush during shutdown
3. **No shutdown checks**: Attempting to send metrics even during shutdown

## Solution Implemented

### 1. Enable Producer Idempotence
```java
// Enable idempotence to prevent OutOfOrderSequenceException during retries
setIfAbsent(producerProps, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
// Increase delivery timeout to handle network issues and retries better
setIfAbsent(producerProps, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
// Set reasonable request timeout
setIfAbsent(producerProps, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
```

### 2. Improved Error Handling
```java
// Log different error types with appropriate levels
if (e instanceof org.apache.kafka.common.errors.OutOfOrderSequenceException) {
    LOGGER.warn("OutOfOrderSequenceException when sending auto balancer metric (this should be resolved with idempotence enabled): {}", e.getMessage());
} else if (e instanceof org.apache.kafka.common.errors.NotLeaderOrFollowerException) {
    LOGGER.warn("NotLeaderOrFollowerException when sending auto balancer metric (transient error): {}", e.getMessage());
} else if (e instanceof InterruptException) {
    LOGGER.info("InterruptException when sending auto balancer metric (likely due to shutdown): {}", e.getMessage());
}
```

### 3. Graceful Shutdown Process
```java
@Override
public void close() {
    LOGGER.info("Closing Auto Balancer metrics reporter, id={}.", brokerId);
    shutdown = true;
    if (metricsReporterRunner != null) {
        metricsReporterRunner.interrupt();
        try {
            metricsReporterRunner.join(PRODUCER_CLOSE_TIMEOUT.toMillis());
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for metrics reporter thread to finish");
            Thread.currentThread().interrupt();
        }
    }
    if (producer != null) {
        try {
            // Try to flush remaining metrics before closing
            producer.flush();
        } catch (Exception e) {
            LOGGER.warn("Failed to flush producer during shutdown: {}", e.getMessage());
        } finally {
            producer.close(PRODUCER_CLOSE_TIMEOUT);
        }
    }
}
```

### 4. Shutdown Checks in Send Method
```java
public void sendAutoBalancerMetric(AutoBalancerMetrics ccm) {
    if (shutdown) {
        return; // Don't send metrics if shutting down
    }
    // ... rest of method
}
```


