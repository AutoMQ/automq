---
last_updated: 2026-05-28
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
