//! eBPF loader and management (Linux only).

#[cfg(all(target_os = "linux", feature = "ebpf"))]
pub mod loader;

#[cfg(all(target_os = "linux", feature = "ebpf"))]
pub mod maps;

#[cfg(all(target_os = "linux", feature = "ebpf"))]
pub use loader::{EbpfLoader, EbpfLoaderConfig, EbpfError};

/// eBPF types matching the C headers
pub mod types {
    use serde::{Deserialize, Serialize};

    /// Device ID size in bytes
    pub const DEVICE_ID_SIZE: usize = 16;

    /// Token bucket for rate limiting
    #[repr(C)]
    #[derive(Debug, Clone, Copy, Default)]
    pub struct TokenBucket {
        pub tokens: u64,
        pub last_refill_ns: u64,
        pub max_tokens: u64,
        pub refill_rate: u64,
    }

    /// Per-CPU statistics
    #[repr(C)]
    #[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
    pub struct EbpfStats {
        pub received: u64,
        pub passed: u64,
        pub dropped_magic: u64,
        pub dropped_rate: u64,
        pub dropped_size: u64,
    }

    /// Event from ring buffer
    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    pub struct KlaisEvent {
        pub device_id: [u8; DEVICE_ID_SIZE],
        pub sequence: u32,
        pub payload_len: u32,
        pub timestamp_ns: u64,
    }

    /// AI configuration
    #[repr(C)]
    #[derive(Debug, Clone, Copy, Default)]
    pub struct AiConfig {
        pub global_rate_limit: u64,
        pub burst_size: u64,
        pub enforce_per_device: u8,
        pub drop_on_anomaly: u8,
        pub _pad: [u8; 6],
    }
}

/// Stub implementations when eBPF is not available
#[cfg(not(feature = "ebpf"))]
pub mod stub {
    use super::types::*;

    /// Stub eBPF manager
    pub struct EbpfManager;

    impl EbpfManager {
        pub fn new() -> Result<Self, &'static str> {
            Err("eBPF feature not enabled")
        }

        pub fn update_config(&self, _config: AiConfig) -> Result<(), &'static str> {
            Err("eBPF feature not enabled")
        }

        pub fn get_stats(&self) -> Result<EbpfStats, &'static str> {
            Err("eBPF feature not enabled")
        }
    }
}

#[cfg(not(feature = "ebpf"))]
pub use stub::EbpfManager;
