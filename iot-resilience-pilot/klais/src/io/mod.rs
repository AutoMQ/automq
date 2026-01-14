//! I/O module for high-performance data path.

pub mod uring;
pub mod xdp;

/// I/O backend selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    /// Standard tokio async I/O (cross-platform)
    Tokio,
    
    /// io_uring (Linux 5.1+)
    IoUring,
    
    /// AF_XDP zero-copy (Linux 4.18+)
    AfXdp,
}

impl Default for IoBackend {
    fn default() -> Self {
        #[cfg(target_os = "linux")]
        {
            // Check kernel version for io_uring support
            Self::IoUring
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            Self::Tokio
        }
    }
}

/// Check if io_uring is available
pub fn io_uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check for io_uring support
        use std::fs;
        fs::read_to_string("/proc/version")
            .map(|v| {
                // Parse kernel version
                if let Some(start) = v.find("Linux version ") {
                    let version_str = &v[start + 14..];
                    if let Some(end) = version_str.find(' ') {
                        let version = &version_str[..end];
                        let parts: Vec<&str> = version.split('.').collect();
                        if parts.len() >= 2 {
                            let major: u32 = parts[0].parse().unwrap_or(0);
                            let minor: u32 = parts[1].parse().unwrap_or(0);
                            return major > 5 || (major == 5 && minor >= 1);
                        }
                    }
                }
                false
            })
            .unwrap_or(false)
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Check if AF_XDP is available
pub fn afxdp_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        fs::read_to_string("/proc/version")
            .map(|v| {
                if let Some(start) = v.find("Linux version ") {
                    let version_str = &v[start + 14..];
                    if let Some(end) = version_str.find(' ') {
                        let version = &version_str[..end];
                        let parts: Vec<&str> = version.split('.').collect();
                        if parts.len() >= 2 {
                            let major: u32 = parts[0].parse().unwrap_or(0);
                            let minor: u32 = parts[1].parse().unwrap_or(0);
                            return major > 4 || (major == 4 && minor >= 18);
                        }
                    }
                }
                false
            })
            .unwrap_or(false)
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Select the best available I/O backend
pub fn auto_select_backend() -> IoBackend {
    if afxdp_available() {
        IoBackend::AfXdp
    } else if io_uring_available() {
        IoBackend::IoUring
    } else {
        IoBackend::Tokio
    }
}
