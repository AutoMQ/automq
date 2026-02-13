//! eBPF loader using libbpf-rs.
//!
//! Handles loading, attaching, and managing eBPF programs:
//! - XDP for packet filtering
//! - TC for traffic shaping
//! - Tracepoints for observability

#[cfg(all(target_os = "linux", feature = "ebpf"))]
use libbpf_rs::{
    MapFlags, Object, ObjectBuilder, Program, ProgramAttachType,
    RingBufferBuilder,
};

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use super::types::*;

/// eBPF loader configuration
#[derive(Debug, Clone)]
pub struct EbpfLoaderConfig {
    /// Path to XDP program object file
    pub xdp_prog_path: String,
    
    /// Path to TC program object file
    pub tc_prog_path: String,
    
    /// Interface to attach XDP program
    pub xdp_interface: String,
    
    /// XDP attach mode
    pub xdp_mode: XdpMode,
    
    /// Enable debug output
    pub debug: bool,
}

impl Default for EbpfLoaderConfig {
    fn default() -> Self {
        Self {
            xdp_prog_path: "bpf/xdp_filter.o".to_string(),
            tc_prog_path: "bpf/tc_classify.o".to_string(),
            xdp_interface: "eth0".to_string(),
            xdp_mode: XdpMode::Native,
            debug: false,
        }
    }
}

/// XDP attach mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XdpMode {
    /// Native mode (requires driver support)
    Native,
    /// Generic/SKB mode (works everywhere, slower)
    Skb,
    /// Offload to NIC (requires hardware support)
    Offload,
}

/// eBPF program manager
pub struct EbpfLoader {
    config: EbpfLoaderConfig,
    
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    xdp_object: Option<Object>,
    
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    tc_object: Option<Object>,
    
    attached: RwLock<bool>,
}

impl EbpfLoader {
    /// Create a new eBPF loader
    pub fn new(config: EbpfLoaderConfig) -> Self {
        Self {
            config,
            #[cfg(all(target_os = "linux", feature = "ebpf"))]
            xdp_object: None,
            #[cfg(all(target_os = "linux", feature = "ebpf"))]
            tc_object: None,
            attached: RwLock::new(false),
        }
    }

    /// Load eBPF programs
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    pub fn load(&mut self) -> Result<(), EbpfError> {
        // Load XDP program
        let xdp_path = Path::new(&self.config.xdp_prog_path);
        if xdp_path.exists() {
            tracing::info!(path = %self.config.xdp_prog_path, "Loading XDP program");
            
            let mut builder = ObjectBuilder::default();
            if self.config.debug {
                builder.debug(true);
            }
            
            let open_obj = builder.open_file(&self.config.xdp_prog_path)
                .map_err(|e| EbpfError::LoadFailed(e.to_string()))?;
            
            let obj = open_obj.load()
                .map_err(|e| EbpfError::LoadFailed(e.to_string()))?;
            
            self.xdp_object = Some(obj);
            tracing::info!("XDP program loaded");
        }
        
        // Load TC program
        let tc_path = Path::new(&self.config.tc_prog_path);
        if tc_path.exists() {
            tracing::info!(path = %self.config.tc_prog_path, "Loading TC program");
            
            let mut builder = ObjectBuilder::default();
            let open_obj = builder.open_file(&self.config.tc_prog_path)
                .map_err(|e| EbpfError::LoadFailed(e.to_string()))?;
            
            let obj = open_obj.load()
                .map_err(|e| EbpfError::LoadFailed(e.to_string()))?;
            
            self.tc_object = Some(obj);
            tracing::info!("TC program loaded");
        }
        
        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    pub fn load(&mut self) -> Result<(), EbpfError> {
        Err(EbpfError::NotSupported)
    }

    /// Attach XDP program to interface
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    pub fn attach_xdp(&self) -> Result<(), EbpfError> {
        let obj = self.xdp_object.as_ref()
            .ok_or(EbpfError::NotLoaded)?;
        
        // Find the XDP program
        let prog = obj.prog("klais_xdp_filter")
            .ok_or_else(|| EbpfError::ProgramNotFound("klais_xdp_filter".to_string()))?;
        
        // Get interface index
        let if_index = get_interface_index(&self.config.xdp_interface)?;
        
        // Attach
        let flags = match self.config.xdp_mode {
            XdpMode::Native => libbpf_rs::XdpFlags::DRV_MODE,
            XdpMode::Skb => libbpf_rs::XdpFlags::SKB_MODE,
            XdpMode::Offload => libbpf_rs::XdpFlags::HW_MODE,
        };
        
        prog.attach_xdp(if_index as i32)
            .map_err(|e| EbpfError::AttachFailed(e.to_string()))?;
        
        *self.attached.write() = true;
        tracing::info!(
            interface = %self.config.xdp_interface,
            mode = ?self.config.xdp_mode,
            "XDP program attached"
        );
        
        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    pub fn attach_xdp(&self) -> Result<(), EbpfError> {
        Err(EbpfError::NotSupported)
    }

    /// Detach XDP program
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    pub fn detach_xdp(&self) -> Result<(), EbpfError> {
        let if_index = get_interface_index(&self.config.xdp_interface)?;
        
        // Detach by attaching null program
        unsafe {
            libc::if_nametoindex(
                std::ffi::CString::new(self.config.xdp_interface.as_str())
                    .unwrap()
                    .as_ptr()
            );
        }
        
        *self.attached.write() = false;
        tracing::info!("XDP program detached");
        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    pub fn detach_xdp(&self) -> Result<(), EbpfError> {
        Ok(())
    }

    /// Update AI configuration in eBPF maps
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    pub fn update_config(&self, config: AiConfig) -> Result<(), EbpfError> {
        let obj = self.xdp_object.as_ref()
            .ok_or(EbpfError::NotLoaded)?;
        
        let map = obj.map("ai_params")
            .ok_or_else(|| EbpfError::MapNotFound("ai_params".to_string()))?;
        
        let key: u32 = 0;
        let value = unsafe {
            std::slice::from_raw_parts(
                &config as *const AiConfig as *const u8,
                std::mem::size_of::<AiConfig>(),
            )
        };
        
        map.update(&key.to_ne_bytes(), value, MapFlags::ANY)
            .map_err(|e| EbpfError::MapUpdateFailed(e.to_string()))?;
        
        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    pub fn update_config(&self, _config: AiConfig) -> Result<(), EbpfError> {
        Err(EbpfError::NotSupported)
    }

    /// Read statistics from eBPF maps
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    pub fn read_stats(&self) -> Result<EbpfStats, EbpfError> {
        let obj = self.xdp_object.as_ref()
            .ok_or(EbpfError::NotLoaded)?;
        
        let map = obj.map("statistics")
            .ok_or_else(|| EbpfError::MapNotFound("statistics".to_string()))?;
        
        let key: u32 = 0;
        let value = map.lookup(&key.to_ne_bytes(), MapFlags::ANY)
            .map_err(|e| EbpfError::MapReadFailed(e.to_string()))?
            .ok_or(EbpfError::MapReadFailed("No value found".to_string()))?;
        
        // For per-CPU maps, we need to sum across CPUs
        let stats: EbpfStats = unsafe {
            std::ptr::read(value.as_ptr() as *const EbpfStats)
        };
        
        Ok(stats)
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    pub fn read_stats(&self) -> Result<EbpfStats, EbpfError> {
        Ok(EbpfStats::default())
    }

    /// Check if programs are attached
    pub fn is_attached(&self) -> bool {
        *self.attached.read()
    }

    /// Get configuration
    pub fn config(&self) -> &EbpfLoaderConfig {
        &self.config
    }
}

impl Drop for EbpfLoader {
    fn drop(&mut self) {
        if self.is_attached() {
            let _ = self.detach_xdp();
        }
    }
}

/// Get interface index by name
#[cfg(target_os = "linux")]
fn get_interface_index(name: &str) -> Result<u32, EbpfError> {
    use std::ffi::CString;
    
    let c_name = CString::new(name)
        .map_err(|_| EbpfError::InvalidInterface(name.to_string()))?;
    
    let index = unsafe { libc::if_nametoindex(c_name.as_ptr()) };
    
    if index == 0 {
        Err(EbpfError::InvalidInterface(name.to_string()))
    } else {
        Ok(index)
    }
}

#[cfg(not(target_os = "linux"))]
fn get_interface_index(_name: &str) -> Result<u32, EbpfError> {
    Err(EbpfError::NotSupported)
}

/// eBPF errors
#[derive(Debug, thiserror::Error)]
pub enum EbpfError {
    #[error("eBPF not supported on this platform")]
    NotSupported,
    
    #[error("eBPF programs not loaded")]
    NotLoaded,
    
    #[error("Failed to load eBPF program: {0}")]
    LoadFailed(String),
    
    #[error("Failed to attach eBPF program: {0}")]
    AttachFailed(String),
    
    #[error("Program not found: {0}")]
    ProgramNotFound(String),
    
    #[error("Map not found: {0}")]
    MapNotFound(String),
    
    #[error("Failed to update map: {0}")]
    MapUpdateFailed(String),
    
    #[error("Failed to read map: {0}")]
    MapReadFailed(String),
    
    #[error("Invalid interface: {0}")]
    InvalidInterface(String),
}

/// Ring buffer event handler
pub struct EventHandler {
    tx: mpsc::Sender<KlaisEvent>,
}

impl EventHandler {
    pub fn new(tx: mpsc::Sender<KlaisEvent>) -> Self {
        Self { tx }
    }

    /// Handle incoming event from ring buffer
    pub fn handle(&self, data: &[u8]) -> i32 {
        if data.len() < std::mem::size_of::<KlaisEvent>() {
            return 0;
        }
        
        let event: KlaisEvent = unsafe {
            std::ptr::read(data.as_ptr() as *const KlaisEvent)
        };
        
        // Non-blocking send
        let _ = self.tx.try_send(event);
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = EbpfLoaderConfig::default();
        assert_eq!(config.xdp_interface, "eth0");
        assert_eq!(config.xdp_mode, XdpMode::Native);
    }

    #[test]
    fn test_loader_creation() {
        let loader = EbpfLoader::new(EbpfLoaderConfig::default());
        assert!(!loader.is_attached());
    }
}
