---
last_updated: 2026-05-29
---

# Review Conventions

## 1. Review Range

- Review pull requests against the target branch full diff, such as
  `origin/1.7...HEAD`.
- Do not limit review to the latest commit unless the user explicitly asks for
  an incremental review.

## 2. Repository Rules

- Check new production and test classes against the Java-first rule in
  `coding-conventions.md` and `testing.md`.
- Check new public class, public method, and test method docs against the
  documentation rules in `coding-conventions.md` and `testing.md`.
- Check AutoMQ changes inside existing Kafka source files for narrow
  `// AutoMQ inject start` and `// AutoMQ inject end` markers.

## 3. Verification

- Before claiming an AutoMQ coding task is done, confirm the required local gate
  from `testing.md` has passed.
- Treat CI failures as review feedback: reproduce or inspect the failing gate,
  fix the cause, and rerun the relevant local command before handing off.
