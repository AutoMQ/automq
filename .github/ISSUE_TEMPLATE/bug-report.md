---
name: "🐛 Bug Report"
about: Report a problem with AutoMQ
title: "[BUG] "
labels: bug
assignees: ''

---

### Version & Environment

<!--

Please provide the following information to help us reproduce and diagnose the issue:

- **AutoMQ Version**: (e.g., `v0.1.0`). You can find this by running `automq --version`.
- **Operating System**: (e.g., `Ubuntu 20.04`). You can get this information from `/etc/os-release` or using the `uname` command.
- **Installation Method**: (e.g., `source`, `binary`, `docker`, `package manager`).
- **Hardware Configuration**: (e.g., CPU, memory, disk type).
- **Other Relevant Software**: (e.g., Kafka version if using Kafka connectors).

-->

### What Went Wrong?

<!--

Describe the unexpected behavior you encountered. Include as much detail as possible, such as error messages, unexpected outputs, or crashes.

For example:
- **Error Message**: "Connection refused: No available brokers"
- **Behavior**: The broker crashes when receiving a large number of messages.

-->

### What Should Have Happened Instead?

<!--

Describe the expected behavior. What did you expect to happen when you encountered the issue?

For example:
- The broker should handle the load without crashing.
- The connection should be established successfully.

-->

### Steps to Reproduce the Issue

<!--

Provide a clear, step-by-step guide on how to reproduce the issue. Include any specific configurations or inputs required.

1. **Step 1**: Start the AutoMQ broker with the default configuration.
2. **Step 2**: Send a large number of messages to the broker using a producer.
3. **Step 3**: Observe the broker's behavior.

If the issue is intermittent, please describe the conditions under which it occurs.

-->

### Additional Information

<!--

Include any additional information that might be helpful in diagnosing the issue. This can include:

- **Logs**: Attach relevant log files or snippets. Please ensure logs are anonymized if they contain sensitive information.
- **Backtraces**: If the issue involves a crash, include the backtrace from the error log.
- **Metric Charts**: Attach any relevant metric charts or performance data.
- **Configuration Files**: Attach relevant configuration files if they are not sensitive.
- **Screenshots**: If applicable, include screenshots of the issue.

For example:
- **Log Snippet**: 
