# Custom Headers for Table Topic REST Catalogs

AutoMQ Table Topic supports custom HTTP headers for REST catalog integrations, enabling integration with catalog services that require specific headers.

## Configuration Pattern

Custom headers can be configured using the following pattern:

```properties
automq.table.topic.catalog.header.{header-name}={header-value}
```

Where:
- `{header-name}` is the HTTP header name (case-sensitive)
- `{header-value}` is the header value

## Common Use Cases

- **Authentication**: Bearer tokens, API keys
- **Multi-tenant environments**: Tenant identification headers
- **Apache Polaris integration**: Realm-specific headers
- **Custom routing**: Load balancer or service routing headers

## Example Configuration

```properties
# Basic REST catalog configuration
automq.table.topic.catalog.type=rest
automq.table.topic.catalog.uri=http://your-catalog-server:8181
automq.table.topic.catalog.warehouse=s3://your-bucket/iceberg

# Custom headers
automq.table.topic.catalog.header.Authorization=Bearer your-token
automq.table.topic.catalog.header.X-Custom-Header=custom-value
```

## Integration Examples

For detailed integration examples, including:
- Apache Polaris setup with docker-compose
- Authentication configurations
- Troubleshooting guides

Visit the [AutoMQ Labs repository](https://github.com/AutoMQ/automq-labs/tree/main/table-topic-solutions).

## Supported Catalog Types

Custom headers are supported only for REST catalogs (`automq.table.topic.catalog.type=rest`).