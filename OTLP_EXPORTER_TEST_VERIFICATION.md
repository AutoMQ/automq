# OTLP Metrics Exporter Issue Verification

## Issue Summary
- **Issue**: [#3111](https://github.com/AutoMQ/automq/issues/3111) - Kafka broker fails to start when enabling OTLP HTTP metrics exporter
- **PR**: [#3124](https://github.com/AutoMQ/automq/pull/3124) - Fix: add missing jdk http sender for otlp metrics exporter
- **Root Cause**: Missing HTTP sender implementation for OpenTelemetry OTLP exporter

## Problem Description
When OTLP HTTP metrics exporter is enabled via `s3.telemetry.metrics.exporter.uri`, the broker crashes during startup with:
```
java.lang.AbstractMethodError: Receiver class io.opentelemetry.exporter.sender.okhttp.internal.OkHttpHttpSenderProvider 
does not define or inherit an implementation of the resolved method 'abstract io.opentelemetry.exporter.internal.http.HttpSender 
createSender(...)'
```

## PR Solution
The PR adds `io.opentelemetry:opentelemetry-exporter-sender-jdk:1.40.0` dependency to the `automq-metrics` module and excludes the problematic `okhttp` sender implementation.

### Changes Made:
1. **build.gradle**: 
   - Added exclusion for `opentelemetry-exporter-sender-okhttp`
   - Added dependency on `opentelemetry-exporter-sender-jdk`

2. **gradle/dependencies.gradle**:
   - Added `opentelemetryExporterSenderJdk` library definition

## Verification Status

### ✅ Build Verification
```bash
./gradlew :automq-metrics:jar
```
**Result**: SUCCESS - Build completed without errors

### ✅ Dependency Verification
```bash
./gradlew :automq-metrics:dependencies | grep -i "opentelemetry-exporter-sender"
```
**Result**: Confirmed `opentelemetry-exporter-sender-jdk:1.40.0` is present in the runtime classpath

### 📝 E2E Test Created
Created comprehensive e2e test: `tests/kafkatest/automq/otlp_metrics_exporter_test.py`

**Test Coverage**:
1. **test_broker_startup_with_otlp_enabled**:
   - Verifies broker starts successfully with OTLP exporter enabled
   - Checks for absence of AbstractMethodError in logs
   - Confirms OTLP exporter initialization
   - Validates basic produce/consume operations

2. **test_otlp_exporter_with_load**:
   - Tests broker stability under production workload
   - Ensures no crashes during load testing
   - Verifies continued operation with OTLP enabled

## Test Execution

### Running the E2E Test

#### Using Docker (Recommended):
```bash
# Build system test libraries
./gradlew systemTestLibs

# Run the specific test
TC_PATHS="tests/kafkatest/automq/otlp_metrics_exporter_test.py" bash tests/docker/run_tests.sh
```

#### Using ducker-ak:
```bash
# Start ducker nodes
bash tests/docker/ducker-ak up

# Run the test
tests/docker/ducker-ak test tests/kafkatest/automq/otlp_metrics_exporter_test.py
```

#### Run specific test method:
```bash
TC_PATHS="tests/kafkatest/automq/otlp_metrics_exporter_test.py::OTLPMetricsExporterTest.test_broker_startup_with_otlp_enabled" bash tests/docker/run_tests.sh
```

## Expected Test Results

### Success Criteria:
1. ✅ Broker starts without AbstractMethodError
2. ✅ OTLP exporter initializes successfully
3. ✅ Log contains: "OTLPMetricsExporter initialized with endpoint: ..."
4. ✅ No crashes during produce/consume operations
5. ✅ Broker remains stable under load

### Failure Indicators (if PR doesn't fix the issue):
- ❌ AbstractMethodError in broker logs
- ❌ Broker fails to start
- ❌ NPE during shutdown due to incomplete initialization
- ❌ Missing OTLP initialization log message

## Branch Information
- **Test Branch**: `test/otlp-metrics-exporter-e2e`
- **PR Branch**: `pr-3124-test` (tracking PR #3124)
- **Base Branch**: `main`

## Commit Information
```
commit c529bc6c0f
Author: [Your Name]
Date: [Date]

test: add e2e test for OTLP metrics exporter startup issue (#3111)

This test verifies the fix for issue #3111 where enabling OTLP HTTP 
metrics exporter caused broker startup failure due to missing HTTP 
sender implementation.
```

## Next Steps

1. **Run the E2E test** to verify the fix works correctly
2. **Review test results** and ensure all assertions pass
3. **Submit the test** as part of PR #3124 or as a separate PR
4. **Document findings** in the issue and PR comments

## Additional Notes

- The test uses a dummy OTLP endpoint (`http://localhost:9090/opentelemetry/v1/metrics`) since we're testing startup, not actual metrics export
- The test is designed to be retained in the codebase as a regression test
- Test requires 4 nodes (1 broker + 3 workers) as specified by `@cluster(num_nodes=4)`
- Test uses ducktape framework which runs in Docker containers

## References
- Issue: https://github.com/AutoMQ/automq/issues/3111
- PR: https://github.com/AutoMQ/automq/pull/3124
- OpenTelemetry Documentation: https://opentelemetry.io/docs/
