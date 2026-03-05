# Build stage
FROM rust:1.75-bookworm AS builder

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    libclang-dev \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy source for dependency caching
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn lib() {}" > src/lib.rs

# Build dependencies (cached layer)
RUN cargo build --release && rm -rf src

# Copy real source
COPY src ./src
COPY bpf ./bpf
COPY build.rs ./

# Touch main.rs to trigger rebuild
RUN touch src/main.rs

# Build for release
RUN cargo build --release --bin klais

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary
COPY --from=builder /app/target/release/klais /usr/local/bin/klais

# Copy eBPF programs (if any)
COPY --from=builder /app/bpf/*.o /app/bpf/ 2>/dev/null || true

# Create non-root user
RUN useradd -r -s /bin/false klais
USER klais

# Expose ports
EXPOSE 5000/udp
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set entrypoint
ENTRYPOINT ["klais"]
