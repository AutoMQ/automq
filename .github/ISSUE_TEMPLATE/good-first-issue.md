---
name: "⭐ Good First Issue"
about: Design good first issue for new contributors
title: "[Good First Issue] "
labels: good first issue
assignees: ''

---

### Background

<!--

Please provide the background for this issue.

For example, currently, Kafka's logs are only stored on the local disk. We want to store them in cloud object storage as well to offer the ability to query logs from object storage. Storing logs in object storage is cheaper and more reliable.

-->

### What's Our Expectation for This Issue

<!--

For example, Local file logs should still exist. When logs are flushed to the local file system, the log data should also be uploaded to object storage. The log path should be like `s3://bucket-name/automq/cluster-id/broker-id/logs/xx`.

-->

### How to Get Started

<!--

Guide the developer on how to complete the issue, including:

For example:
- Precondition:
    - You need to understand how AutoMQ prints logs to the local file system.
- Main classes involved when coding:
    - `ExampleClassA`
    - `ExampleClassB`
- Other tips:
    - You can refer to the `ExampleClassA` and `ExampleClassB` in AutoMQ for inspiration.
-->

### Reference
- [Kafka Official Documentation](https://kafka.apache.org/documentation/)
