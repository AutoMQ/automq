# Fix for Issue #2615: Failed to init cert metrics

## Problem
AutoMQ was failing to initialize certificate metrics with the error:
```
java.lang.IllegalArgumentException: Illegal base64 character 20
```

The error occurred in the `S3StreamKafkaMetricsManager.parseCertificates()` method when trying to decode PEM certificate content that contained whitespace characters (spaces, tabs, newlines, carriage returns).

## Root Cause
The original implementation only removed newlines (`\n`) from the PEM certificate content before Base64 decoding:
```java
pemPart.replace("-----BEGIN CERTIFICATE-----", "").replaceAll("\\n", "")
```

However, PEM certificates can contain various whitespace characters including:
- Spaces (ASCII 32)
- Tabs (ASCII 9) 
- Carriage returns (ASCII 13)
- Newlines (ASCII 10)

Character 20 (space) in the error message was causing the Base64 decoder to fail.

## Solution
### 1. Fixed the PEM parsing logic
Updated the `parseCertificates()` method to:
- Remove ALL whitespace characters using `replaceAll("\\s", "")` instead of just newlines
- Added graceful error handling for both `IllegalArgumentException` (Base64 errors) and `CertificateException` (certificate parsing errors)
- Changed from fixed array to dynamic List to handle cases where some certificates fail to parse
- Added proper logging for failed certificate parsing attempts

### 2. Key Changes Made
**File**: `server-common/src/main/java/org/apache/kafka/server/metrics/s3stream/S3StreamKafkaMetricsManager.java`

**Before**:
```java
private static X509Certificate[] parseCertificates(String pemContent) throws CertificateException {
    // ... 
    byte[] certBytes = Base64.getDecoder().decode(pemPart.replace("-----BEGIN CERTIFICATE-----", "").replaceAll("\\n", ""));
    // ...
}
```

**After**:
```java
private static X509Certificate[] parseCertificates(String pemContent) throws CertificateException {
    // ...
    String cleanedPemPart = pemPart.replace("-----BEGIN CERTIFICATE-----", "")
            .replaceAll("\\s", ""); // Remove all whitespace characters
    
    try {
        byte[] certBytes = Base64.getDecoder().decode(cleanedPemPart);
        // ...
    } catch (IllegalArgumentException e) {
        LOGGER.warn("Failed to decode certificate part due to invalid Base64, skipping: {}", e.getMessage());
    } catch (CertificateException e) {
        LOGGER.warn("Failed to parse certificate, skipping: {}", e.getMessage());
    }
    // ...
}
```

### 3. Added Comprehensive Tests
Created `S3StreamKafkaMetricsManagerTest.java` with tests for:
- Empty certificate content
- Certificates with various whitespace issues (spaces, tabs, carriage returns)
- Invalid Base64 content
- Graceful error handling

## Impact
- **Fixes the crash**: AutoMQ will no longer crash when initializing certificate metrics with whitespace-containing PEM content
- **Robust error handling**: Invalid certificates are now skipped with appropriate logging instead of causing complete failure
- **Backward compatible**: The fix doesn't break existing functionality
- **Better logging**: Administrators can now see which certificates failed to parse and why

## Testing
- All existing tests continue to pass
- New tests verify the fix handles whitespace correctly
- Tested with various certificate formats and edge cases

This fix resolves the "Illegal base64 character 20" error reported in issue #2615 and makes the certificate parsing more robust overall.
