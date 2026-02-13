//! Linux-specific high-performance optimizations
//!
//! Features:
//! - SO_REUSEPORT: Kernel-level load balancing across sockets
//! - recvmmsg: Batch UDP receives (up to 64 packets per syscall)  
//! - CPU affinity: Pin workers to specific cores
//! - Huge pages: Pre-fault memory for reduced TLB misses

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Set SO_REUSEPORT on a socket for kernel-level load balancing
#[cfg(target_os = "linux")]
pub fn set_reuseport(socket: &tokio::net::UdpSocket) -> std::io::Result<()> {
    use std::os::fd::AsFd;
    use std::os::unix::io::AsRawFd;

    let fd = socket.as_fd().as_raw_fd();

    unsafe {
        let optval: libc::c_int = 1;
        let ret = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );

        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_reuseport(_socket: &tokio::net::UdpSocket) -> std::io::Result<()> {
    // No-op on non-Linux
    Ok(())
}

/// Set CPU affinity for the current thread
#[cfg(target_os = "linux")]
pub fn set_cpu_affinity(cpu_id: usize) -> std::io::Result<()> {
    unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu_id, &mut cpuset);

        let ret = libc::sched_setaffinity(
            0, // current thread
            std::mem::size_of::<libc::cpu_set_t>(),
            &cpuset,
        );

        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_cpu_affinity(_cpu_id: usize) -> std::io::Result<()> {
    // No-op on non-Linux
    Ok(())
}

/// Increase socket receive buffer size
#[cfg(target_os = "linux")]
pub fn set_recv_buffer_size(socket: &tokio::net::UdpSocket, size: usize) -> std::io::Result<()> {
    use std::os::fd::AsFd;
    use std::os::unix::io::AsRawFd;

    let fd = socket.as_fd().as_raw_fd();

    unsafe {
        let optval: libc::c_int = size as libc::c_int;
        let ret = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );

        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_recv_buffer_size(_socket: &tokio::net::UdpSocket, _size: usize) -> std::io::Result<()> {
    Ok(())
}

/// Batch UDP receiver using recvmmsg (Linux-only)
/// 
/// This is significantly faster than individual recv_from calls
/// as it amortizes syscall overhead across multiple packets.
#[cfg(target_os = "linux")]
pub struct BatchReceiver {
    fd: libc::c_int,
    buffers: Vec<Vec<u8>>,
    iovecs: Vec<libc::iovec>,
    msgs: Vec<libc::mmsghdr>,
    addrs: Vec<libc::sockaddr_storage>,
}

#[cfg(target_os = "linux")]
impl BatchReceiver {
    /// Create a new batch receiver
    /// 
    /// # Arguments
    /// * `socket` - The UDP socket to receive from
    /// * `batch_size` - Number of packets to receive per syscall (max 64)
    /// * `packet_size` - Maximum size of each packet
    pub fn new(socket: &tokio::net::UdpSocket, batch_size: usize, packet_size: usize) -> Self {
        use std::os::fd::AsFd;
        use std::os::unix::io::AsRawFd;

        let batch_size = batch_size.min(64);
        let fd = socket.as_fd().as_raw_fd();

        let mut buffers: Vec<Vec<u8>> = (0..batch_size)
            .map(|_| vec![0u8; packet_size])
            .collect();

        let mut iovecs: Vec<libc::iovec> = buffers
            .iter_mut()
            .map(|buf| libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect();

        let mut addrs: Vec<libc::sockaddr_storage> = (0..batch_size)
            .map(|_| unsafe { std::mem::zeroed() })
            .collect();

        let msgs: Vec<libc::mmsghdr> = (0..batch_size)
            .map(|i| libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: &mut addrs[i] as *mut _ as *mut libc::c_void,
                    msg_namelen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
                    msg_iov: &mut iovecs[i],
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            })
            .collect();

        Self {
            fd,
            buffers,
            iovecs,
            msgs,
            addrs,
        }
    }

    /// Receive a batch of packets
    /// 
    /// Returns an iterator over (data_slice, length) for each received packet.
    /// This is a blocking call.
    pub fn recv_batch(&mut self) -> std::io::Result<impl Iterator<Item = &[u8]>> {
        // Reset message lengths
        for msg in &mut self.msgs {
            msg.msg_len = 0;
        }

        let received = unsafe {
            libc::recvmmsg(
                self.fd,
                self.msgs.as_mut_ptr(),
                self.msgs.len() as libc::c_uint,
                libc::MSG_WAITFORONE, // Return after first message, but get more if available
                std::ptr::null_mut(), // No timeout
            )
        };

        if received < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let count = received as usize;

        Ok((0..count).map(move |i| {
            let len = self.msgs[i].msg_len as usize;
            &self.buffers[i][..len]
        }))
    }
}

/// Get the number of available CPU cores
pub fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// Optimize the current process for network I/O
#[cfg(target_os = "linux")]
pub fn optimize_process() {
    // Try to set SCHED_FIFO for real-time priority (requires CAP_SYS_NICE)
    unsafe {
        let param = libc::sched_param {
            sched_priority: 1,
        };
        let _ = libc::sched_setscheduler(0, libc::SCHED_FIFO, &param);
    }

    // Disable ASLR for more predictable memory layout (optional)
    // This is generally not recommended for security reasons

    // Lock memory to prevent swapping
    unsafe {
        let _ = libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn optimize_process() {
    // No-op on non-Linux
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_num_cpus() {
        let cpus = num_cpus();
        assert!(cpus >= 1);
    }
}
