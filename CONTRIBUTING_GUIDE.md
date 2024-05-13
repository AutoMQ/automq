# Contributing to AutoMQ

Thank you for your interest in contributing! We love community contributions.
Read on to learn how to contribute to AutoMQ.
We appreciate first time contributors and we are happy to assist you in getting started. In case of questions, just
reach out to us via [Wechat Group](https://www.automq.com/img/----------------------------1.png)
or [Slack](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA)!

Before getting started, please review AutoMQ's Code of Conduct. Everyone interacting in Slack or Wechat
follow [Code of Conduct](../community/code-of-conduct.md).

## Code Contributions

Most of the issues open for contributions are tagged with 'good first issue.' To claim one, simply reply with 'pick up,' and the AutoMQ maintainers will assign the issue to you. If you have any questions about the 'good first issue,' please feel free to ask. We will do our best to clarify any doubts you may have.
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
8. Write a PR title and description that follows the [Pull Request Handbook](./resources/pull-requests-handbook.md)
   and [Pull Request Template](https://github.com/airbytehq/airbyte/blob/master/.github/pull_request_template.md).
9. An AutoMQ maintainer will trigger the CI tests for you and review the code.
10. Review and respond to feedback and questions by AutoMQ maintainers.
11. Merge the contribution.

Pull Request reviews are done on a regular basis.

:::info
Please make sure you respond to our feedback/questions and sign our CLA.

Pull Requests without updates will be closed due inactivity.
:::

Guidelines to common code contributions:

- [Submit code change to existing Source Connector](change-cdk-connector.md)
- [Submit a New Connector](submit-new-connector.md)

## Requirement

| Requirement            | Version    |
|------------------------|------------|
| Compiling requirements | JDK 17     |
| Compiling requirements | Scale 2.13 |
| Running requirements   | JDK 17     |

## Documentation

We welcome Pull Requests that enhance the grammar, structure, or fix typos in our documentation.

## Engage with the Community

Another crucial way to contribute is by reporting bugs and helping other users in the community.

You're welcome to enter
the [Community Slack](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA) and help other
users or report bugs in Github.

## Attribution

This contributing document is adapted from that of [AutoMQ](https://github.com/airbytehq/airbyte).
