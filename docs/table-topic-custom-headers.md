# Custom Headers Configuration for Table Topic Catalogs

AutoMQ Table Topic supports custom HTTP headers for REST catalog integrations, enabling seamless integration with catalog services that require specific headers such as Apache Polaris, authentication tokens, or tenant identification.

## Overview

When using Table Topic with REST-based Iceberg catalogs, you can configure custom HTTP headers that will be included in all catalog requests. This is particularly useful for:

- **Apache Polaris** integration requiring `Polaris-Realm` headers
- **Authentication** using custom bearer tokens or API keys
- **Multi-tenant** environments requiring tenant identification headers
- **Custom routing** or load balancing headers

## Configuration Pattern

Custom headers are configured using the following pattern:

```properties
automq.table.topic.catalog.header.{header-name}={header-value}
```

Where:
- `{header-name}` is the HTTP header name (case-sensitive)
- `{header-value}` is the header value

## Apache Polaris Integration

[Apache Polaris](https://polaris.apache.org/) requires realm-specific headers for multi-tenant environments. Configure AutoMQ to work with Polaris as follows:

```properties
# Basic REST catalog configuration
automq.table.topic.catalog.type=rest
automq.table.topic.catalog.uri=http://your-polaris-server:8181
automq.table.topic.catalog.warehouse=s3://your-bucket/iceberg

# Polaris-specific realm header
automq.table.topic.catalog.header.Polaris-Realm=your-realm-name

# Optional: Additional Polaris headers
automq.table.topic.catalog.header.Authorization=Bearer your-token
```

### Polaris Configuration Details

According to the [Polaris production configuration guide](https://polaris.apache.org/releases/1.1.0/configuring-polaris-for-production/):

- If a request contains the `Polaris-Realm` header, Polaris uses the specified realm
- If the realm is not in the allowed list, Polaris returns a 404 Not Found response
- If no header is present, Polaris uses the first realm in the list as default

With AutoMQ's header configuration, you can explicitly set the realm:

```properties
automq.table.topic.catalog.header.Polaris-Realm=PRODUCTION
```

## Common Use Cases

### Authentication Headers

```properties
# Bearer token authentication
automq.table.topic.catalog.header.Authorization=Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# API key authentication
automq.table.topic.catalog.header.X-API-Key=your-api-key
```

### Multi-tenant Environments

```properties
# Tenant identification
automq.table.topic.catalog.header.X-Tenant-ID=tenant-123
automq.table.topic.catalog.header.X-Organization=my-org
```

### Custom Routing

```properties
# Load balancer routing
automq.table.topic.catalog.header.X-Route-To=datacenter-west
automq.table.topic.catalog.header.X-Service-Version=v2
```

### Content Type and Accept Headers

```properties
# Explicit content type (usually handled automatically)
automq.table.topic.catalog.header.Content-Type=application/json
automq.table.topic.catalog.header.Accept=application/json
```

## Complete Configuration Example

Here's a comprehensive example combining multiple header types:

```properties
# S3 and general configuration
s3.data.buckets=0@s3://my-bucket?region=us-east-1&endpoint=http://localhost:9000

# Table Topic catalog configuration
automq.table.topic.catalog.type=rest
automq.table.topic.catalog.uri=http://polaris-server:8181
automq.table.topic.catalog.warehouse=s3://my-bucket/iceberg

# Custom headers for Polaris integration
automq.table.topic.catalog.header.Polaris-Realm=PRODUCTION
automq.table.topic.catalog.header.Authorization=Bearer your-jwt-token
automq.table.topic.catalog.header.X-Tenant-ID=tenant-production
automq.table.topic.catalog.header.X-Client-Version=automq-1.6.0
```

## Supported Catalog Types

Custom headers are supported for the following catalog types:

- ✅ **REST** (`automq.table.topic.catalog.type=rest`) - Full support
- ❌ **AWS Glue** (`automq.table.topic.catalog.type=glue`) - Not applicable (uses AWS SDK)
- ❌ **Hive Metastore** (`automq.table.topic.catalog.type=hive`) - Not applicable (uses Thrift)
- ❌ **Nessie** (`automq.table.topic.catalog.type=nessie`) - Uses REST but may have specific auth mechanisms
- ❌ **Table Bucket** (`automq.table.topic.catalog.type=tablebucket`) - Uses AWS SDK

> **Note**: Custom headers are primarily designed for REST catalog integrations where HTTP headers are the standard mechanism for metadata and authentication.

## Troubleshooting

### Header Not Being Sent

1. **Verify Configuration**: Ensure the header configuration follows the exact pattern:
   ```properties
   automq.table.topic.catalog.header.Header-Name=header-value
   ```

2. **Check Catalog Type**: Custom headers only work with `type=rest`

3. **Case Sensitivity**: HTTP header names are case-sensitive. Use the exact casing required by your catalog service.

### Polaris Returns 404 Not Found

1. **Verify Realm Name**: Ensure the realm name in the header matches exactly with your Polaris configuration
2. **Check Allowed Realms**: Verify that the realm is in Polaris's allowed realms list
3. **Network Connectivity**: Ensure AutoMQ can reach the Polaris server

### Authentication Failures

1. **Token Expiry**: Check if your bearer token or API key has expired
2. **Token Format**: Ensure the token format matches what your catalog expects
3. **Permissions**: Verify the token has the necessary permissions for catalog operations

## Implementation Details

Custom headers are implemented in the `CatalogFactory` class and are passed through to the underlying Apache Iceberg REST catalog implementation. The headers are included in all HTTP requests made to the catalog service, including:

- Configuration requests (`GET /v1/config`)
- Namespace operations (`GET /v1/namespaces`, `POST /v1/namespaces`)
- Table operations (`GET /v1/namespaces/{namespace}/tables`, `POST /v1/namespaces/{namespace}/tables`)
- Metadata operations

## Testing Your Configuration

To verify that your custom headers are working correctly:

1. **Enable Debug Logging**: Add the following to your log4j configuration:
   ```properties
   log4j.logger.org.apache.iceberg.rest=DEBUG
   ```

2. **Monitor Network Traffic**: Use tools like `tcpdump` or Wireshark to inspect HTTP requests

3. **Check Catalog Server Logs**: Review your catalog server logs to ensure headers are being received

4. **Test with curl**: Manually test your catalog endpoint with the same headers:
   ```bash
   curl -H "Polaris-Realm: your-realm" \
        -H "Authorization: Bearer your-token" \
        "http://your-polaris-server:8181/v1/config?warehouse=s3://bucket/iceberg"
   ```

## References

- [Apache Polaris Documentation](https://polaris.apache.org/)
- [Apache Iceberg REST Catalog Specification](https://iceberg.apache.org/docs/latest/rest/)
- [AutoMQ Table Topic Documentation](https://www.automq.com/docs/automq/table-topic/)
- [GitHub Issue #2890](https://github.com/AutoMQ/automq/issues/2890)
