# AutoMQ Agent Instructions

This repository is the open-source AutoMQ Kafka fork.

Follow this file first when working with coding agents. The detailed,
progressively loaded rules live in `.agents/`.

## Context Loading

- Before changing production code, read `.agents/coding-conventions.md`.
- Before adding or changing tests, read `.agents/testing.md`.
- Before reviewing or handing off a PR, read `.agents/review.md`.

## Core Rules

- Keep changes scoped to the requested task and the existing Kafka/AutoMQ module
  boundaries.
- Follow `.agents/coding-conventions.md` for language choice, public docs, scope
  control, and Kafka fork markers.
- Follow `.agents/testing.md` for focused tests, tagged test selection, required
  completion gates, SpotBugs, E2E suite registration, and suppression rules.
- Follow `.agents/review.md` for PR review range, repository rule checks, and
  CI failure handling.

## Verification

Do not claim an AutoMQ coding task is complete without running the relevant
verification from `.agents/testing.md` and recording any command that could not
be run.
