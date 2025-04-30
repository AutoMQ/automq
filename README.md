# Contributing to AutoMQ

Thank you for your interest in contributing! We welcome and appreciate community contributions.  
Read on to learn how to get started with AutoMQ. If you have any questions, feel free to reach out via our [WeChat group](https://www.automq.com/img/----------------------------1.png) or [Slack workspace](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA).

Before you begin, please review AutoMQ’s [Code of Conduct](CODE_OF_CONDUCT.md). Everyone who interacts on Slack or WeChat must follow the Code of Conduct.

---

## Code Contributions

Most open issues are labeled **good first issue**. To claim one, simply comment **“pick up”** on the issue, and an AutoMQ maintainer will assign it to you. If you have any questions about a **good first issue**, feel free to ask—we’re here to help!

Start here:  
[Issues tagged “good first issue”](https://github.com/AutoMQ/automq-for-kafka/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)

### Typical Workflow

1. **Fork** the AutoMQ repository.
2. **Clone** your fork locally.
3. **Create** a branch named `{your_username}/{feature_or_bug}` (e.g., `jdoe/source-stock-api-stream-fix`).
4. **Implement** your changes and commit them.
5. **Push** your branch to your fork.
6. **Open** a Pull Request (PR) against the main AutoMQ repository.
7. **Link** an existing issue (without the `needs triage` label) to your PR. PRs without a linked issue will be closed.
8. **Use** the [Pull Request Template](PULL_REQUEST_TEMPLATE.md) for your PR title and description.
9. An AutoMQ maintainer will trigger CI tests and review your code.
10. **Address** any feedback or questions from maintainers.
11. **Merge** your contribution once approved.

> **Note:** Please respond promptly to feedback and sign our Contributor License Agreement (CLA). PRs left inactive will be closed.

---

## Requirements

| Requirement          | Version |
| -------------------- | ------- |
| Java Development Kit | 17      |
| Scala                | 2.13    |

> **Tip:** See the [Scala 2.13.12 download page](https://www.scala-lang.org/download/2.13.12.html) for installation instructions.

---

## Local Development with IntelliJ IDEA

### Using Gradle

AutoMQ uses Gradle (via the `gradlew` wrapper) just like Apache Kafka:

```bash
./gradlew jar -x test
```
