//! NUMA (Non-Uniform Memory Access) topology awareness.
//!
//! Optimizes memory allocation and thread placement for:
//! - Local memory access on multi-socket systems
//! - CPU affinity for hot paths
//! - Isolation of latency-critical threads

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// NUMA node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaNode {
    /// Node ID
    pub id: usize,
    
    /// CPUs in this node
    pub cpus: Vec<usize>,
    
    /// Total memory in bytes
    pub memory_total: u64,
    
    /// Free memory in bytes
    pub memory_free: u64,
    
    /// Distance to other nodes (latency metric)
    pub distances: HashMap<usize, usize>,
}

/// NUMA topology
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaTopology {
    /// All NUMA nodes
    pub nodes: Vec<NumaNode>,
    
    /// Total CPU count
    pub total_cpus: usize,
    
    /// Is this a NUMA system?
    pub is_numa: bool,
}

impl NumaTopology {
    /// Detect NUMA topology from the system
    pub fn detect() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux()
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            Self::single_node()
        }
    }

    /// Create a single-node non-NUMA topology
    pub fn single_node() -> Self {
        let cpu_count = num_cpus::get();
        
        Self {
            nodes: vec![NumaNode {
                id: 0,
                cpus: (0..cpu_count).collect(),
                memory_total: 0,
                memory_free: 0,
                distances: HashMap::new(),
            }],
            total_cpus: cpu_count,
            is_numa: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn detect_linux() -> Self {
        use std::fs;
        
        let mut nodes = Vec::new();
        let mut total_cpus = 0;
        
        // Read NUMA nodes from sysfs
        let numa_path = "/sys/devices/system/node";
        
        if let Ok(entries) = fs::read_dir(numa_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("node") {
                    if let Ok(node_id) = name[4..].parse::<usize>() {
                        if let Some(node) = Self::read_node(node_id) {
                            total_cpus += node.cpus.len();
                            nodes.push(node);
                        }
                    }
                }
            }
        }
        
        if nodes.is_empty() {
            return Self::single_node();
        }
        
        nodes.sort_by_key(|n| n.id);
        
        Self {
            is_numa: nodes.len() > 1,
            nodes,
            total_cpus,
        }
    }

    #[cfg(target_os = "linux")]
    fn read_node(node_id: usize) -> Option<NumaNode> {
        use std::fs;
        
        let node_path = format!("/sys/devices/system/node/node{}", node_id);
        
        // Read CPUs
        let cpulist_path = format!("{}/cpulist", node_path);
        let cpus = if let Ok(content) = fs::read_to_string(&cpulist_path) {
            parse_cpu_list(&content)
        } else {
            Vec::new()
        };
        
        // Read memory info
        let meminfo_path = format!("{}/meminfo", node_path);
        let (memory_total, memory_free) = if let Ok(content) = fs::read_to_string(&meminfo_path) {
            parse_meminfo(&content)
        } else {
            (0, 0)
        };
        
        // Read distances
        let distance_path = format!("{}/distance", node_path);
        let distances = if let Ok(content) = fs::read_to_string(&distance_path) {
            content
                .split_whitespace()
                .enumerate()
                .filter_map(|(i, d)| d.parse().ok().map(|dist| (i, dist)))
                .collect()
        } else {
            HashMap::new()
        };
        
        Some(NumaNode {
            id: node_id,
            cpus,
            memory_total,
            memory_free,
            distances,
        })
    }

    /// Get node with most available memory
    pub fn best_memory_node(&self) -> Option<&NumaNode> {
        self.nodes.iter().max_by_key(|n| n.memory_free)
    }

    /// Get node for a specific CPU
    pub fn node_for_cpu(&self, cpu: usize) -> Option<&NumaNode> {
        self.nodes.iter().find(|n| n.cpus.contains(&cpu))
    }

    /// Get CPUs for a node
    pub fn cpus_for_node(&self, node_id: usize) -> Option<&[usize]> {
        self.nodes.iter()
            .find(|n| n.id == node_id)
            .map(|n| n.cpus.as_slice())
    }

    /// Get isolated CPUs for latency-critical work
    pub fn isolated_cpus(&self) -> Vec<usize> {
        #[cfg(target_os = "linux")]
        {
            use std::fs;
            
            if let Ok(content) = fs::read_to_string("/sys/devices/system/cpu/isolated") {
                return parse_cpu_list(&content);
            }
        }
        
        Vec::new()
    }
}

/// Parse CPU list format (e.g., "0-3,8-11")
fn parse_cpu_list(s: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    
    for part in s.trim().split(',') {
        if part.contains('-') {
            let range: Vec<&str> = part.split('-').collect();
            if range.len() == 2 {
                if let (Ok(start), Ok(end)) = (range[0].parse::<usize>(), range[1].parse::<usize>()) {
                    for cpu in start..=end {
                        cpus.push(cpu);
                    }
                }
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    
    cpus
}

/// Parse /proc/meminfo format
fn parse_meminfo(s: &str) -> (u64, u64) {
    let mut total = 0u64;
    let mut free = 0u64;
    
    for line in s.lines() {
        if line.contains("MemTotal:") {
            if let Some(val) = extract_kb_value(line) {
                total = val * 1024;
            }
        } else if line.contains("MemFree:") {
            if let Some(val) = extract_kb_value(line) {
                free = val * 1024;
            }
        }
    }
    
    (total, free)
}

fn extract_kb_value(line: &str) -> Option<u64> {
    line.split_whitespace()
        .nth(3)
        .and_then(|s| s.parse().ok())
}

/// CPU affinity manager
pub struct AffinityManager {
    topology: NumaTopology,
}

impl AffinityManager {
    /// Create a new affinity manager
    pub fn new() -> Self {
        Self {
            topology: NumaTopology::detect(),
        }
    }

    /// Get the topology
    pub fn topology(&self) -> &NumaTopology {
        &self.topology
    }

    /// Pin current thread to specific CPUs
    #[cfg(target_os = "linux")]
    pub fn pin_to_cpus(&self, cpus: &[usize]) -> std::io::Result<()> {
        use std::mem;
        
        let mut cpuset: libc::cpu_set_t = unsafe { mem::zeroed() };
        
        for &cpu in cpus {
            unsafe {
                libc::CPU_SET(cpu, &mut cpuset);
            }
        }
        
        let result = unsafe {
            libc::sched_setaffinity(
                0,
                mem::size_of::<libc::cpu_set_t>(),
                &cpuset,
            )
        };
        
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn pin_to_cpus(&self, _cpus: &[usize]) -> std::io::Result<()> {
        Ok(())  // No-op on non-Linux
    }

    /// Pin current thread to a NUMA node
    pub fn pin_to_node(&self, node_id: usize) -> std::io::Result<()> {
        if let Some(cpus) = self.topology.cpus_for_node(node_id) {
            self.pin_to_cpus(cpus)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("NUMA node {} not found", node_id),
            ))
        }
    }

    /// Suggest thread placement strategy
    pub fn suggest_placement(&self, thread_count: usize) -> ThreadPlacement {
        if !self.topology.is_numa {
            // Single-node: spread across all CPUs
            return ThreadPlacement {
                threads: (0..thread_count)
                    .map(|i| ThreadAssignment {
                        thread_id: i,
                        cpus: vec![i % self.topology.total_cpus],
                        numa_node: 0,
                    })
                    .collect(),
            };
        }
        
        // Multi-node: distribute across nodes, prefer local memory
        let threads_per_node = thread_count / self.topology.nodes.len();
        let mut assignments = Vec::new();
        let mut thread_id = 0;
        
        for node in &self.topology.nodes {
            let node_threads = if node.id == self.topology.nodes.len() - 1 {
                thread_count - thread_id  // Remainder to last node
            } else {
                threads_per_node
            };
            
            for i in 0..node_threads {
                let cpu = node.cpus.get(i % node.cpus.len()).copied().unwrap_or(0);
                assignments.push(ThreadAssignment {
                    thread_id,
                    cpus: vec![cpu],
                    numa_node: node.id,
                });
                thread_id += 1;
            }
        }
        
        ThreadPlacement { threads: assignments }
    }
}

impl Default for AffinityManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread placement strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadPlacement {
    pub threads: Vec<ThreadAssignment>,
}

/// Single thread assignment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadAssignment {
    pub thread_id: usize,
    pub cpus: Vec<usize>,
    pub numa_node: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpu_list() {
        assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpu_list("0,2,4"), vec![0, 2, 4]);
        assert_eq!(parse_cpu_list("0-1,4-5"), vec![0, 1, 4, 5]);
    }

    #[test]
    fn test_numa_detection() {
        let topo = NumaTopology::detect();
        assert!(topo.total_cpus > 0);
        assert!(!topo.nodes.is_empty());
    }

    #[test]
    fn test_single_node() {
        let topo = NumaTopology::single_node();
        assert!(!topo.is_numa);
        assert_eq!(topo.nodes.len(), 1);
    }

    #[test]
    fn test_suggest_placement() {
        let manager = AffinityManager::new();
        let placement = manager.suggest_placement(4);
        
        assert_eq!(placement.threads.len(), 4);
        for (i, thread) in placement.threads.iter().enumerate() {
            assert_eq!(thread.thread_id, i);
            assert!(!thread.cpus.is_empty());
        }
    }
}
