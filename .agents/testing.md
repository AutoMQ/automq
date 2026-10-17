---
last_updated: 2026-05-29
---

# Testing Conventions

## 1. Language Choice

- New AutoMQ tests are Java-first.
- Add Scala tests only when extending an existing Scala test class is clearly
  lower-risk than creating a Java test.

## 2. Test Documentation

- Test method docs are written in English.
- Each new test method should describe the scenario or contract being protected.
- Prefer Given/When/Then wording for rule-heavy behavior and edge cases.
- Avoid comments that only restate the test method name.

## 3. Tagged Test Selection

- AutoMQ CI commonly runs tagged test tasks instead of every Gradle test task.
- New focused AutoMQ unit tests should be tagged with `@Tag("S3Unit")` when
  they are expected to run in the tagged S3Unit selection.
- When verifying AutoMQ changes locally, prefer the workflow-style tagged
  selection when the change is broad enough:

```bash
./gradlew --build-cache metadata:S3UnitTest core:S3UnitTest s3stream:test
```

- For narrow changes, run focused `core:S3UnitTest --tests ...` first, then
  broaden to the workflow-style selection before handing off.
- When adding E2E coverage, include the test in the appropriate
  `tests/suites/automq_test_suite*.yml` suite so the CI selection can discover
  it.

## 4. Required Completion Gate

- Before claiming an AutoMQ coding task is done, run the CI-style formatting,
  license, and checkstyle gate:

```bash
./gradlew --build-cache rat checkstyleMain checkstyleTest spotlessJavaCheck
```

- Do not replace this gate with focused unit tests. Run it in addition to the
  relevant focused or tagged test selection for the change.

## 5. Checkstyle Suppression

- Use narrow `@SuppressWarnings` only when the rule conflicts with a deliberate
  structure, such as enum-switch hot-path classification, schema dispatch, or a
  test class that must cover many generated protocol data types together.
- Prefer fixing ordinary style issues, unused imports, missing locale, and
  low-cost complexity reductions instead of suppressing them.
- When suppressing a Checkstyle rule in code, keep the annotation at the
  smallest practical scope and include the `checkstyle:` prefix when suppressing
  a specific method-level Checkstyle rule.
