# Contributing to KLAIS

Thank you for your interest in contributing to KLAIS! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Style Guidelines](#style-guidelines)

---

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/). By participating, you are expected to uphold this code.

---

## Getting Started

### Prerequisites

- **Rust 1.75+**: Install via [rustup](https://rustup.rs/)
- **Linux 5.15+**: Required for eBPF features
- **clang**: For eBPF program compilation
- **librdkafka**: Kafka client library

### Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/klais.git
cd klais

# Add upstream remote
git remote add upstream https://github.com/ORIGINAL_OWNER/klais.git
```

---

## Development Setup

### Install Dependencies

```bash
# Ubuntu/Debian
sudo apt-get install -y \
    build-essential \
    cmake \
    libclang-dev \
    librdkafka-dev \
    pkg-config \
    linux-headers-$(uname -r)

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install development tools
cargo install cargo-watch cargo-audit cargo-criterion
```

### Build

```bash
# Debug build
cargo build

# Release build
cargo build --release

# With all features
cargo build --features full
```

### Run

```bash
# Run with defaults
cargo run

# Run with logging
RUST_LOG=debug cargo run

# Run with custom config
KLAIS_UDP_BIND=0.0.0.0:5000 cargo run
```

---

## Making Changes

### Branch Naming

```
feature/short-description    # New features
fix/issue-number-description # Bug fixes
docs/what-you-documented     # Documentation
perf/what-you-optimized      # Performance improvements
refactor/what-you-refactored # Code refactoring
```

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `perf`: Performance improvement
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance

**Examples**:

```
feat(dam): add per-device rate limiting

Implement per-device token buckets using DashMap for O(1) lookup.
Each device gets independent rate limiting based on device_id hash.

Closes #42
```

```
fix(kafka): handle broker disconnection gracefully

Add circuit breaker to prevent cascade failures when
Kafka brokers become unreachable.
```

---

## Testing

### Run Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_dam_filter

# With logging
RUST_LOG=debug cargo test -- --nocapture

# Integration tests only
cargo test --test '*'
```

### Run Benchmarks

```bash
# All benchmarks
cargo bench

# Specific benchmark
cargo bench --bench throughput
```

### Code Coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate coverage report
cargo tarpaulin --out Html
```

### Test Categories

| Category | Location | Purpose |
|----------|----------|---------|
| Unit tests | `src/*.rs` | Module-level tests |
| Integration tests | `tests/` | Cross-module tests |
| Benchmarks | `benches/` | Performance tests |
| Property tests | `tests/proptest_*.rs` | Fuzzing |

---

## Pull Request Process

### Before Submitting

1. **Sync with upstream**:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run checks**:
   ```bash
   cargo fmt --check
   cargo clippy -- -D warnings
   cargo test
   cargo doc --no-deps
   ```

3. **Update documentation** if needed

4. **Add tests** for new functionality

### PR Template

```markdown
## Description
Brief description of changes.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-reviewed
- [ ] Documentation updated
- [ ] No new warnings
```

### Review Process

1. Create PR against `main` branch
2. Automated CI checks run
3. Maintainer review
4. Address feedback
5. Squash and merge

---

## Style Guidelines

### Rust

We follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/).

```bash
# Format code
cargo fmt

# Lint
cargo clippy -- -D warnings
```

**Key Points**:

```rust
// Use explicit types for public APIs
pub fn process(packet: &KlaisPacket) -> Result<(), ProcessError> { ... }

// Document all public items
/// Processes a KLAIS packet through the dam filter.
///
/// # Arguments
/// * `packet` - The parsed packet to process
///
/// # Returns
/// * `Ok(())` if packet was accepted
/// * `Err(ProcessError)` if packet was rejected
pub fn process(packet: &KlaisPacket) -> Result<(), ProcessError> { ... }

// Use meaningful error types
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("rate limit exceeded")]
    RateLimited,
    
    #[error("queue full")]
    QueueFull,
}
```

### eBPF/C

```c
// Use snake_case for functions and variables
int klais_xdp_filter(struct xdp_md *ctx) { ... }

// Document BPF maps
/* Per-device rate limiting buckets
 * Key: FNV-1a hash of device_id
 * Value: token_bucket structure
 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    ...
} device_limits SEC(".maps");
```

### Documentation

- Use Markdown for all documentation
- Include code examples
- Keep diagrams in ASCII art for portability
- Update README for user-facing changes

---

## Architecture Decisions

When proposing significant changes, create an ADR (Architecture Decision Record):

```markdown
# ADR-NNN: Title

## Status
Proposed | Accepted | Deprecated | Superseded

## Context
What is the issue?

## Decision
What decision was made?

## Consequences
What are the results?
```

---

## Getting Help

- **Issues**: For bugs and feature requests
- **Discussions**: For questions and ideas
- **Discord**: Real-time chat (link in README)

---

## Recognition

Contributors are recognized in:
- `CONTRIBUTORS.md`
- Release notes
- Project README

Thank you for contributing to KLAIS! 🚀
