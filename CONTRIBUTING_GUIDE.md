# Contributing to AutoMQ

Thank you for your interest in contributing! We love community contributions.
Read on to learn how to contribute to AutoMQ.
We appreciate first time contributors and we are happy to assist you in getting started. In case of questions, just
reach out to us via [Wechat Group](https://www.automq.com/img/----------------------------1.png)
or [Slack](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA)!

Before getting started, please review AutoMQ's Code of Conduct. Everyone interacting in Slack or Wechat
follow [Code of Conduct](CODE_OF_CONDUCT.md).

## Code Contributions

Most of the issues open for contributions are tagged with 'good first issue.' To claim one, simply reply with 'pick up' in the issue and the AutoMQ maintainers will assign the issue to you. If you have any questions about the 'good first issue' please feel free to ask. We will do our best to clarify any doubts you may have.
Start with
this [tagged good first issue](https://github.com/AutoMQ/automq-for-kafka/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)

The usual workflow of code contribution is:

1. Fork the AutoMQ repository.
2. Clone the repository locally.
3. Create a branch for your feature/bug fix with the format `{YOUR_USERNAME}/{FEATURE/BUG}` (
   e.g. `jdoe/source-stock-api-stream-fix`)
4. Make and commit changes.
5. Push your local branch to your fork.
6. Submit a Pull Request so that we can review your changes.
7. [Link an existing Issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue)
   that does not include the `needs triage` label to your Pull Request. A pull request without a linked issue will be
   closed, otherwise.
8. Write a PR title and description that follows the [Pull Request Template](PULL_REQUEST_TEMPLATE.md).
9. An AutoMQ maintainer will trigger the CI tests for you and review the code.
10. Review and respond to feedback and questions by AutoMQ maintainers.
11. Merge the contribution.

Pull Request reviews are done on a regular basis.

:::info
Please make sure you respond to our feedback/questions and sign our CLA.

Pull Requests without updates will be closed due inactivity.
:::

## Requirement

| Requirement            | Version    |
|------------------------|------------|
| Compiling requirements | JDK 17     |
| Compiling requirements | Scale 2.13 |
| Running requirements   | JDK 17     |

> Tips: You can refer the [document](https://www.scala-lang.org/download/2.13.12.html) to install Scale 2.13

## Local Debug with IDEA

### Gradle

Build AutoMQ is the same with Apache Kafka. Kafka uses Gradle as its project management tool. The management of Gradle projects is based on scripts written in Groovy syntax, and within the Kafka project, the main project management configuration is found in the build.gradle file located in the root directory, which serves a similar function to the root POM in Maven projects. Gradle also supports configuring a build.gradle for each module separately, but Kafka does not do this; all modules are managed by the build.gradle file in the root directory.

It is not recommended to manually install Gradle. The gradlew script in the root directory will automatically download Gradle for you, and the version is also specified by the gradlew script.

### Build
```
./gradlew jar -x test
```

### Prepare S3 service
Refer this [doc](https://docs.localstack.cloud/getting-started/installation/) to install localstack to mock a local s3 service or use AWS S3 service directly. 

If you are using localstack then create a bucket with the following command:
```
aws s3api create-bucket --bucket ko3 --endpoint=http://127.0.0.1:4566
```
### Modify Configuration

Modify the `config/kraft/server.properties` file. The following settings need to be changed:

```
s3.endpoint=https://s3.amazonaws.com

# The region of S3 service
# For Aliyun, you have to set the region to aws-global. See https://www.alibabacloud.com/help/zh/oss/developer-reference/use-amazon-s3-sdks-to-access-oss.
s3.region=us-east-1

# The bucket of S3 service to store data
s3.bucket=ko3
```
> Tips: If you're using localstack, make sure to set the s3.endpoint to http://127.0.0.1:4566, not localhost. Set the region to us-east-1. The bucket should match the one created earlier.

### Format
Generated Cluster UUID:
```
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```
Format Metadata Catalog:
```
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```
### IDEA Start Configuration
| Item            | Value    |
|------------------------|------------|
| Main | core/src/main/scala/kafka/Kafka.scala     |
| ClassPath | -cp kafka.core.main |
| VM Options   | -Xmx1 -Xms1G -server -XX:+UseZGC -XX:MaxDirectMemorySize=2G -Dkafka.logs.dir=logs/ -Dlog4j.configuration=file:config/log4j.properties -Dio.netty.leakDetection.level=paranoid    |
| CLI Arguments | config/kraft/server.properties|
| Environment | KAFKA_S3_ACCESS_KEY=test;KAFKA_S3_SECRET_KEY=test |

> tips: If you are using localstack, just use any value of access key and secret key. If you are using real S3 service, set `KAFKA_S3_ACCESS_KEY` and `KAFKA_S3_SECRET_KEY` to the real access key and secret key that have read/write permission of S3 service.


## Documentation

We welcome Pull Requests that enhance the grammar, structure, or fix typos in our documentation.

## Engage with the Community

Another crucial way to contribute is by reporting bugs and helping other users in the community.

You're welcome to enter
the [Community Slack](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA) and help other
users or report bugs in GitHub.

## Attribution

This contributing document is adapted from that of [Airbyte](https://github.com/airbytehq/airbyte).
