# Changelog

All notable changes to KLAIS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial implementation of KLAIS kernel-level AI system
- Binary protocol parser with magic header validation
- Dam Filter with token bucket rate limiting and overflow queue
- Per-device rate limiting using FNV-1a hashing
- UDP receiver with tokio async runtime
- Kafka producer with batching and compression
- XDP packet filtering program
- eBPF loader with libbpf-rs integration
- LLM-based traffic analysis with GGML-style prompts
- Device fingerprinting and anomaly detection
- Thundering herd prediction
- PID controller for adaptive rate limiting
- NUMA topology detection and CPU affinity management
- Circuit breaker pattern for resilience
- Latency histograms and tracing
- io_uring integration for zero-copy I/O
- AF_XDP socket abstraction
- REST API with health, config, stats, and metrics endpoints
- Prometheus metrics integration
- Docker deployment with docker-compose
- Comprehensive documentation

### Changed
- N/A (initial release)

### Deprecated
- N/A

### Removed
- N/A

### Fixed
- N/A

### Security
- N/A

---

## [0.1.0] - 2026-01-14

### Added
- Initial release with core functionality
- Protocol parsing and dam filtering
- Kafka integration
- Basic inference and control plane

---

## Release Notes Template

### [X.Y.Z] - YYYY-MM-DD

#### Highlights
Brief summary of the most important changes.

#### Breaking Changes
- List any breaking changes here

#### Migration Guide
Steps to migrate from previous version.

#### New Features
- Feature 1
- Feature 2

#### Bug Fixes
- Fix 1
- Fix 2

#### Performance Improvements
- Improvement 1

#### Documentation
- Doc update 1

#### Dependencies
- Dependency updates
