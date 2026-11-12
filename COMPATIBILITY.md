# Compatibility with Apache Kafka

AutoMQ is a next-generation Apache Kafka® distribution redesigned based on cloud-native concepts. It is fully compatible with the Apache Kafka® protocol and features.

## Architecture & Compatibility Approach

In terms of technical architecture, AutoMQ reuses the compute layer code of Apache Kafka® and only replaces minimal components at the storage layer. This ensures complete compatibility with the relevant versions of Apache Kafka®. Applications based on Apache Kafka® can be seamlessly migrated to AutoMQ.

AutoMQ employs a micro-layer storage replacement method to adapt to Apache Kafka, enabling rapid updates to new community versions and supporting the latest Apache Kafka version updates as quickly as within **T+1 month**.

During the compatibility verification phase, AutoMQ used Apache Kafka® test case projects and successfully passed the relevant version tests.

## Version Compatibility Matrix

The following table shows the compatibility relationship between AutoMQ releases and the Apache Kafka versions they are based on:

| AutoMQ Version | Based on Apache Kafka | Backward Compatible With Kafka Clients | Status          |
|----------------|-----------------------|----------------------------------------|-----------------|
| v1.6.x         | 3.9.x                | 0.9.x – 3.9.x                         | Latest (v1.6.5) |
| v1.5.x         | 3.8.x                | 0.9.x – 3.8.x                         | Stable          |
| v1.4.x         | 3.8.x                | 0.9.x – 3.8.x                         | Stable          |
| v1.1.x – v1.3.x | 3.7.x              | 0.9.x – 3.7.x                         | Stable          |
| v1.0.x         | 3.4.x                | 0.9.x – 3.4.x                         | Legacy          |

> **Note:** AutoMQ versions align directly with Apache Kafka versions. Each AutoMQ version is compatible with the Kafka Client, Connector, Kafka Streams, and other components within the Apache Kafka ecosystem for the corresponding Kafka version range.

## Kafka 4.x Compatibility

As of AutoMQ v1.6.5, the project has **not** yet adopted Apache Kafka 4.x. The current latest adapted version is Apache Kafka **3.9.x**. Once Apache Kafka 4.x support is implemented, this document will be updated accordingly.

## Ecosystem Compatibility

AutoMQ is compatible with the following components from the Apache Kafka ecosystem:

- **Kafka Clients** (Producer / Consumer API)
- **Kafka Connect** (Source & Sink Connectors)
- **Kafka Streams**
- **Schema Registry** (Confluent-compatible)
- **Kafka Proxy**
- **MirrorMaker 2**

## Further Resources

- [AutoMQ Documentation](https://www.automq.com/docs/automq/what-is-automq/overview)
- [AutoMQ GitHub Repository](https://github.com/AutoMQ/automq)
- [Apache Kafka Compatibility Wiki](https://github.com/AutoMQ/automq/wiki/Compatibility-with-Apache-Kafka) *(Note: Wiki might be outdated)*

## Trademarks

Apache®, Apache Kafka®, Kafka®, and associated open source project names are trademarks of the Apache Software Foundation.
