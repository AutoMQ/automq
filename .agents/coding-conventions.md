---
last_updated: 2026-05-28
---

# Coding Conventions

## 1. Language Choice

- AutoMQ runtime code is Java-first.
- New production classes should be Java unless they are tightly extending an
  existing Scala-only Kafka integration surface.
- Avoid adding new Scala runtime types for standalone AutoMQ behavior.
- If a change touches an existing Scala class, keep the edit local and do not
  migrate unrelated code just to satisfy the Java-first rule.

## 2. Documentation

- Code comments and Javadocs are written in English.
- New public classes should have class docs that describe responsibility and
  boundary.
- New public methods should have method docs that describe behavior, inputs,
  outputs, state changes, lifecycle effects, or failure semantics when relevant.
- Avoid comments that only repeat the method or field name.

## 3. Scope Control

- Prefer existing Kafka/AutoMQ abstractions and module boundaries.
- Do not introduce new dependencies when the JDK or existing project utilities
  are enough.
- For stateful runtime code, make ownership of lifecycle, concurrency, and
  cleanup explicit in the API or class documentation.

## 4. Kafka Fork Markers

- When adding or changing AutoMQ logic inside an existing Kafka source file,
  wrap the logic with `// AutoMQ inject start` and `// AutoMQ inject end`.
- Do not add inject markers around import-only changes.
- Keep marker ranges small so future Kafka upstream merges can identify the
  AutoMQ-owned behavior.
