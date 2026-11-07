---
last_updated: 2026-10-30
---

# Pull Request Description Conventions

The PR title and description form the squashed commit message. Write them in
English as durable engineering history for future readers, without relying on
the review conversation. Put transient information such as reviewer pings,
current CI status, and coordination notes in PR comments.

Do not add a Verification, Testing, or Test Plan section to the PR description.
Put verification commands, pass/fail results, skipped or unavailable checks,
and detailed validation reports in PR comments instead. This does not relax
the verification requirements in `testing.md`. Test behavior may be described
in the explanation of the change when it helps reviewers understand the
contract being protected.

Use the following template for non-trivial changes. Subsections are optional;
add them only when they improve clarity. Small or mechanical changes may use a
shorter form. Do not leave the guidance comments in the final description.

```markdown
## Why

<!--
Describe the background and current behavior, then identify the concrete problem
and requirement or goal. Include material evidence, constraints, or non-goals
when they clarify the scope.
-->

## What

<!--
Describe the chosen solution and resulting system behavior at a high level.
State the scope and any important behavior that remains unchanged. Leave
implementation mechanics to How.
-->

## How

<!--
Describe the abstract control flow, data flow, state transitions, and ownership;
do not translate the diff file by file. When relevant, cover correctness
invariants and fallback behavior, compatibility and version gating,
configuration changes, observability changes, and rollout or rollback
considerations. Add subsections only when they improve clarity.
-->

## Reviewer notes

<!--
Give a suggested reading order through the core flow using stable file paths or
class names. Explain the responsibility, contract, or invariant implemented by
each group, and call out cross-cutting changes that need special attention.
Avoid line numbers and temporary review status.
-->
```
