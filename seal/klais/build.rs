//! Build script for KLAIS.
//!
//! Handles:
//! - eBPF program compilation (if enabled)
//! - Build information embedding
//! - Feature detection

use std::env;
use std::process::Command;

fn main() {
    // Embed build information
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=bpf/");
    
    // Git hash
    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
    {
        if output.status.success() {
            let hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
            println!("cargo:rustc-env=GIT_HASH={}", hash);
        }
    }
    
    // Build timestamp
    let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
    println!("cargo:rustc-env=BUILD_TIME={}", timestamp);
    
    // Rust version
    if let Ok(output) = Command::new("rustc").args(["--version"]).output() {
        if output.status.success() {
            let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
            println!("cargo:rustc-env=RUSTC_VERSION={}", version);
        }
    }
    
    // Target triple
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=TARGET={}", target);
    
    // Compile eBPF programs if feature enabled and on Linux
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    compile_ebpf();
}

#[cfg(all(target_os = "linux", feature = "ebpf"))]
fn compile_ebpf() {
    use std::path::Path;
    
    let bpf_dir = Path::new("bpf");
    let out_dir = env::var("OUT_DIR").unwrap();
    
    let sources = ["xdp_filter.c", "tc_classify.c"];
    
    for source in &sources {
        let source_path = bpf_dir.join(source);
        if !source_path.exists() {
            continue;
        }
        
        let output_name = source.replace(".c", ".o");
        let output_path = Path::new(&out_dir).join(&output_name);
        
        println!("cargo:rerun-if-changed={}", source_path.display());
        
        let status = Command::new("clang")
            .args([
                "-O2",
                "-target", "bpf",
                "-c",
                "-g",
                "-D__TARGET_ARCH_x86",
                &source_path.to_string_lossy(),
                "-o",
                &output_path.to_string_lossy(),
            ])
            .status();
        
        match status {
            Ok(s) if s.success() => {
                println!("cargo:warning=Compiled eBPF program: {}", source);
            }
            Ok(s) => {
                println!("cargo:warning=Failed to compile {}: exit code {:?}", source, s.code());
            }
            Err(e) => {
                println!("cargo:warning=Failed to run clang for {}: {}", source, e);
            }
        }
    }
}
