---
name: "⭐ Good first issue"
about: Design good first issue for new contributors
title: "[Good first issue] "
labels: good first issue
assignees: ''

---

### Background

<!--

Please give your issue background.
e.g. Now AutoMQ Kafka's logs are only stored in the local disk. We want to store them in the cloud object storage as well to offer the ability to query logs from object storage. Store logs on the object storage is cheaper and more reliable.

-->

### What's our expectation for the issue

<!--

e.g. Local file logs still exist. When log is flushed to local file system, the log data will upload to object storage as well. The log path will be like `s3://bucket-name/automq/cluster-id/broker-id/logs/xx`. 

-->

### How to started
<!--

Guide the developer how to complete the issue,including:

e.g.
- Precondition: 
    - You need to know the principal of how AutoMQ print logs to local file system.
- What main classes are involved when you are coding:
    - `ExampleClassA`
    - `ExampleClassB`
- Other tips: 
    - You can refer to the `ExampleClassA` and `ExampleClassB` of AutoMQ Kafka to get some inspiration.
-->

### Reference
- [Kafka Official Document](https://kafka.apache.org/documentation/)
