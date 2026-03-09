//! UDP receiver for KLAIS ingestion gateway.
//!
//! High-performance async UDP listener with:
//! - Zero-copy parsing where possible
//! - Configurable buffer sizes
//! - Integration with Dam filter

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::UdpSocket;

use crate::dam::DamFilter;
use crate::protocol::{parse_packet, ParseError};

/// UDP receiver statistics
#[derive(Debug, Default)]
pub struct UdpStats {
    /// Total datagrams received
    pub received: AtomicU64,
    
    /// Datagrams successfully parsed
    pub parsed: AtomicU64,
    
    /// Datagrams with parse errors
    pub parse_errors: AtomicU64,
    
    /// Bytes received
    pub bytes_received: AtomicU64,
}

/// UDP receiver configuration
#[derive(Debug, Clone)]
pub struct UdpConfig {
    /// Bind address
    pub bind_addr: String,
    
    /// Receive buffer size in bytes
    pub recv_buffer_size: usize,
    
    /// Socket receive buffer (SO_RCVBUF)
    pub socket_buffer_size: Option<usize>,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            bind_addr: crate::defaults::UDP_BIND_ADDR.to_string(),
            recv_buffer_size: 65536,  // Max UDP datagram size
            socket_buffer_size: Some(16 * 1024 * 1024),  // 16MB
        }
    }
}

/// UDP receiver
pub struct UdpReceiver {
    socket: UdpSocket,
    dam: Arc<DamFilter>,
    stats: Arc<UdpStats>,
    recv_buffer_size: usize,
}

impl UdpReceiver {
    /// Create and bind a new UDP receiver
    pub async fn bind(config: UdpConfig, dam: Arc<DamFilter>) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(&config.bind_addr).await?;
        
        // Set socket options for high performance
        #[cfg(unix)]
        if let Some(buf_size) = config.socket_buffer_size {
            use std::os::unix::io::AsRawFd;
            let fd = socket.as_raw_fd();
            unsafe {
                let size = buf_size as libc::c_int;
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &size as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
        
        tracing::info!(addr = %config.bind_addr, "UDP receiver bound");
        
        Ok(Self {
            socket,
            dam,
            stats: Arc::new(UdpStats::default()),
            recv_buffer_size: config.recv_buffer_size,
        })
    }

    /// Get local address
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Get statistics handle
    pub fn stats(&self) -> Arc<UdpStats> {
        Arc::clone(&self.stats)
    }

    /// Run the receive loop (blocking)
    pub async fn run(&self) -> std::io::Result<()> {
        let mut buf = vec![0u8; self.recv_buffer_size];
        
        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    self.stats.received.fetch_add(1, Ordering::Relaxed);
                    self.stats.bytes_received.fetch_add(len as u64, Ordering::Relaxed);
                    
                    self.process_datagram(&buf[..len], addr).await;
                }
                Err(e) => {
                    tracing::error!(error = %e, "UDP recv error");
                }
            }
        }
    }

    /// Process a single datagram
    async fn process_datagram(&self, data: &[u8], addr: SocketAddr) {
        match parse_packet(data) {
            Ok(packet) => {
                self.stats.parsed.fetch_add(1, Ordering::Relaxed);
                
                // Pass through the dam filter
                let result = self.dam.try_pass(packet).await;
                
                tracing::trace!(
                    addr = %addr,
                    result = ?result,
                    "Packet processed"
                );
            }
            Err(e) => {
                self.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                
                tracing::debug!(
                    addr = %addr,
                    error = %e,
                    "Parse error"
                );
            }
        }
    }
}

/// Spawn multiple receiver workers for parallel processing
pub async fn spawn_receivers(
    config: UdpConfig,
    dam: Arc<DamFilter>,
    worker_count: usize,
) -> std::io::Result<Vec<tokio::task::JoinHandle<std::io::Result<()>>>> {
    let mut handles = Vec::with_capacity(worker_count);
    
    // For true parallel UDP receiving, we'd use SO_REUSEPORT
    // For now, we create a single receiver
    let receiver = Arc::new(UdpReceiver::bind(config, dam).await?);
    
    for i in 0..worker_count {
        let receiver = Arc::clone(&receiver);
        let handle = tokio::spawn(async move {
            tracing::info!(worker = i, "UDP worker started");
            receiver.run().await
        });
        handles.push(handle);
    }
    
    Ok(handles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use crate::dam::DamConfig;

    #[tokio::test]
    async fn test_udp_bind() {
        let (tx, _rx) = mpsc::channel(100);
        let dam = Arc::new(DamFilter::new(DamConfig::default(), tx));
        
        let config = UdpConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        
        let receiver = UdpReceiver::bind(config, dam).await.unwrap();
        let addr = receiver.local_addr().unwrap();
        assert!(addr.port() > 0);
    }
}
