//! AF_XDP (XDP Sockets) for zero-copy packet reception.
//!
//! AF_XDP provides a kernel bypass path for packet I/O, enabling:
//! - Zero-copy between NIC and userspace
//! - Millions of packets per second per core
//! - Direct integration with XDP programs

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// AF_XDP socket configuration
#[derive(Debug, Clone)]
pub struct AfXdpConfig {
    /// Interface name (e.g., "eth0")
    pub interface: String,
    
    /// Queue ID to attach to
    pub queue_id: u32,
    
    /// Number of frames in UMEM
    pub frame_count: u32,
    
    /// Frame size (usually 4096 or 2048)
    pub frame_size: u32,
    
    /// Fill ring size
    pub fill_size: u32,
    
    /// Completion ring size
    pub comp_size: u32,
    
    /// RX ring size
    pub rx_size: u32,
    
    /// TX ring size
    pub tx_size: u32,
    
    /// Use huge pages for UMEM
    pub huge_pages: bool,
    
    /// Enable zero-copy mode
    pub zero_copy: bool,
    
    /// Enable busy polling
    pub busy_poll: bool,
}

impl Default for AfXdpConfig {
    fn default() -> Self {
        Self {
            interface: "eth0".to_string(),
            queue_id: 0,
            frame_count: 4096,
            frame_size: 4096,
            fill_size: 2048,
            comp_size: 2048,
            rx_size: 2048,
            tx_size: 2048,
            huge_pages: false,
            zero_copy: true,
            busy_poll: true,
        }
    }
}

/// UMEM (User Memory) region for AF_XDP
#[derive(Debug)]
pub struct Umem {
    /// Memory region
    memory: Vec<u8>,
    
    /// Frame size
    frame_size: usize,
    
    /// Number of frames
    frame_count: usize,
    
    /// Free frame indices
    free_frames: crossbeam::queue::ArrayQueue<u64>,
    
    /// Statistics
    stats: UmemStats,
}

/// UMEM statistics
#[derive(Debug, Default)]
pub struct UmemStats {
    pub frames_allocated: AtomicU64,
    pub frames_freed: AtomicU64,
    pub allocation_failures: AtomicU64,
}

impl Umem {
    /// Create a new UMEM region
    pub fn new(frame_count: usize, frame_size: usize) -> Self {
        let total_size = frame_count * frame_size;
        let memory = vec![0u8; total_size];
        
        let free_frames = crossbeam::queue::ArrayQueue::new(frame_count);
        for i in 0..frame_count {
            let _ = free_frames.push((i * frame_size) as u64);
        }
        
        Self {
            memory,
            frame_size,
            frame_count,
            free_frames,
            stats: UmemStats::default(),
        }
    }

    /// Allocate a frame, returns the address offset
    pub fn alloc_frame(&self) -> Option<u64> {
        match self.free_frames.pop() {
            Some(addr) => {
                self.stats.frames_allocated.fetch_add(1, Ordering::Relaxed);
                Some(addr)
            }
            None => {
                self.stats.allocation_failures.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Free a frame
    pub fn free_frame(&self, addr: u64) {
        let _ = self.free_frames.push(addr);
        self.stats.frames_freed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get frame data
    pub fn get_frame(&self, addr: u64) -> Option<&[u8]> {
        let offset = addr as usize;
        if offset + self.frame_size <= self.memory.len() {
            Some(&self.memory[offset..offset + self.frame_size])
        } else {
            None
        }
    }

    /// Get mutable frame data
    pub fn get_frame_mut(&mut self, addr: u64) -> Option<&mut [u8]> {
        let offset = addr as usize;
        if offset + self.frame_size <= self.memory.len() {
            Some(&mut self.memory[offset..offset + self.frame_size])
        } else {
            None
        }
    }

    /// Get frame size
    pub fn frame_size(&self) -> usize {
        self.frame_size
    }

    /// Get frame count
    pub fn frame_count(&self) -> usize {
        self.frame_count
    }

    /// Get available frames
    pub fn available(&self) -> usize {
        self.free_frames.len()
    }

    /// Get memory base pointer
    pub fn base_ptr(&self) -> *const u8 {
        self.memory.as_ptr()
    }

    /// Get total memory size
    pub fn total_size(&self) -> usize {
        self.memory.len()
    }
}

/// Ring buffer descriptor
#[derive(Debug)]
pub struct RingDesc {
    /// Address in UMEM
    pub addr: u64,
    
    /// Length of data
    pub len: u32,
    
    /// Options/flags
    pub options: u32,
}

/// Producer ring (for TX and Fill)
#[derive(Debug)]
pub struct ProducerRing {
    /// Ring entries
    entries: Vec<u64>,
    
    /// Producer index
    producer: AtomicU64,
    
    /// Cached consumer index
    cached_consumer: u64,
    
    /// Ring mask
    mask: u64,
}

impl ProducerRing {
    /// Create a new producer ring
    pub fn new(size: usize) -> Self {
        assert!(size.is_power_of_two());
        Self {
            entries: vec![0; size],
            producer: AtomicU64::new(0),
            cached_consumer: 0,
            mask: (size - 1) as u64,
        }
    }

    /// Reserve entries for production
    pub fn reserve(&mut self, count: usize) -> Option<u64> {
        let prod = self.producer.load(Ordering::Relaxed);
        let avail = self.entries.len() as u64 - (prod - self.cached_consumer);
        
        if avail >= count as u64 {
            Some(prod)
        } else {
            None
        }
    }

    /// Submit produced entries  
    pub fn submit(&self, count: usize) {
        self.producer.fetch_add(count as u64, Ordering::Release);
    }

    /// Get entry at index
    pub fn get_mut(&mut self, idx: u64) -> &mut u64 {
        &mut self.entries[(idx & self.mask) as usize]
    }
}

/// Consumer ring (for RX and Completion)
#[derive(Debug)]
pub struct ConsumerRing {
    /// Ring entries
    entries: Vec<RingDesc>,
    
    /// Consumer index
    consumer: AtomicU64,
    
    /// Cached producer index
    cached_producer: u64,
    
    /// Ring mask
    mask: u64,
}

impl ConsumerRing {
    /// Create a new consumer ring
    pub fn new(size: usize) -> Self {
        assert!(size.is_power_of_two());
        Self {
            entries: (0..size).map(|_| RingDesc { addr: 0, len: 0, options: 0 }).collect(),
            consumer: AtomicU64::new(0),
            cached_producer: 0,
            mask: (size - 1) as u64,
        }
    }

    /// Peek available entries
    pub fn peek(&self) -> usize {
        let cons = self.consumer.load(Ordering::Relaxed);
        (self.cached_producer - cons) as usize
    }

    /// Consume entries
    pub fn consume(&self, count: usize) {
        self.consumer.fetch_add(count as u64, Ordering::Release);
    }

    /// Get entry at index
    pub fn get(&self, idx: u64) -> &RingDesc {
        &self.entries[(idx & self.mask) as usize]
    }
}

/// AF_XDP socket (stub for non-Linux or when AF_XDP is unavailable)
#[derive(Debug)]
pub struct XdpSocket {
    config: AfXdpConfig,
    umem: Umem,
    fill_ring: ProducerRing,
    comp_ring: ConsumerRing,
    rx_ring: ConsumerRing,
    tx_ring: ProducerRing,
    stats: XdpSocketStats,
}

/// Socket statistics
#[derive(Debug, Default)]
pub struct XdpSocketStats {
    pub rx_packets: AtomicU64,
    pub tx_packets: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub rx_dropped: AtomicU64,
    pub tx_dropped: AtomicU64,
}

impl XdpSocket {
    /// Create a new XDP socket (simulation mode for portability)
    pub fn new(config: AfXdpConfig) -> std::io::Result<Self> {
        let umem = Umem::new(
            config.frame_count as usize,
            config.frame_size as usize,
        );
        
        let fill_ring = ProducerRing::new(config.fill_size as usize);
        let comp_ring = ConsumerRing::new(config.comp_size as usize);
        let rx_ring = ConsumerRing::new(config.rx_size as usize);
        let tx_ring = ProducerRing::new(config.tx_size as usize);
        
        Ok(Self {
            config,
            umem,
            fill_ring,
            comp_ring,
            rx_ring,
            tx_ring,
            stats: XdpSocketStats::default(),
        })
    }

    /// Fill the fill ring with available frames
    pub fn fill_frames(&mut self, count: usize) -> usize {
        let mut filled = 0;
        
        if let Some(idx) = self.fill_ring.reserve(count) {
            for i in 0..count {
                if let Some(addr) = self.umem.alloc_frame() {
                    *self.fill_ring.get_mut(idx + i as u64) = addr;
                    filled += 1;
                } else {
                    break;
                }
            }
            
            if filled > 0 {
                self.fill_ring.submit(filled);
            }
        }
        
        filled
    }

    /// Receive packets
    pub fn receive<F>(&mut self, mut handler: F) -> usize
    where
        F: FnMut(&[u8]),
    {
        let available = self.rx_ring.peek();
        if available == 0 {
            return 0;
        }
        
        let cons = self.rx_ring.consumer.load(Ordering::Relaxed);
        
        for i in 0..available {
            let desc = self.rx_ring.get(cons + i as u64);
            
            if let Some(frame) = self.umem.get_frame(desc.addr) {
                handler(&frame[..desc.len as usize]);
                self.stats.rx_packets.fetch_add(1, Ordering::Relaxed);
                self.stats.rx_bytes.fetch_add(desc.len as u64, Ordering::Relaxed);
            }
        }
        
        self.rx_ring.consume(available);
        available
    }

    /// Get UMEM reference
    pub fn umem(&self) -> &Umem {
        &self.umem
    }

    /// Get mutable UMEM reference
    pub fn umem_mut(&mut self) -> &mut Umem {
        &mut self.umem
    }

    /// Get statistics
    pub fn stats(&self) -> &XdpSocketStats {
        &self.stats
    }

    /// Get configuration
    pub fn config(&self) -> &AfXdpConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_umem_creation() {
        let umem = Umem::new(16, 4096);
        
        assert_eq!(umem.frame_count(), 16);
        assert_eq!(umem.frame_size(), 4096);
        assert_eq!(umem.total_size(), 16 * 4096);
        assert_eq!(umem.available(), 16);
    }

    #[test]
    fn test_umem_alloc_free() {
        let umem = Umem::new(4, 4096);
        
        let addr1 = umem.alloc_frame().unwrap();
        let addr2 = umem.alloc_frame().unwrap();
        
        assert_eq!(umem.available(), 2);
        assert_ne!(addr1, addr2);
        
        umem.free_frame(addr1);
        assert_eq!(umem.available(), 3);
    }

    #[test]
    fn test_xdp_socket_creation() {
        let config = AfXdpConfig {
            frame_count: 64,
            frame_size: 2048,
            fill_size: 32,
            comp_size: 32,
            rx_size: 32,
            tx_size: 32,
            ..Default::default()
        };
        
        let socket = XdpSocket::new(config).unwrap();
        assert_eq!(socket.umem().frame_count(), 64);
    }

    #[test]
    fn test_fill_frames() {
        let config = AfXdpConfig {
            frame_count: 64,
            frame_size: 2048,
            fill_size: 32,
            ..Default::default()
        };
        
        let mut socket = XdpSocket::new(config).unwrap();
        let filled = socket.fill_frames(16);
        
        assert_eq!(filled, 16);
        assert_eq!(socket.umem().available(), 64 - 16);
    }
}
