/* KLAIS Shared Header
 *
 * Structures shared between eBPF programs and userspace Rust code.
 * Keep in sync with src/ebpf/types.rs
 */

#ifndef __KLAIS_H__
#define __KLAIS_H__

/* Protocol constants */
#define KLAIS_MAGIC        0xAA55
#define DEVICE_ID_SIZE     16
#define MIN_PACKET_SIZE    22  /* magic(2) + device_id(16) + seq(4) */

/* KLAIS Packet Header */
struct klais_header {
    __u16 magic;
    __u8  device_id[DEVICE_ID_SIZE];
    __u32 sequence;
} __attribute__((packed));

/* Token bucket for rate limiting */
struct token_bucket {
    __u64 tokens;
    __u64 last_refill_ns;
    __u64 max_tokens;
    __u64 refill_rate;
};

/* Per-CPU statistics */
struct stats {
    __u64 received;
    __u64 passed;
    __u64 dropped_magic;
    __u64 dropped_rate;
    __u64 dropped_size;
};

/* Event sent to userspace */
struct klais_event {
    __u8  device_id[DEVICE_ID_SIZE];
    __u32 sequence;
    __u32 payload_len;
    __u64 timestamp_ns;
};

/* AI configuration */
struct ai_config {
    __u64 global_rate_limit;
    __u64 burst_size;
    __u8  enforce_per_device;
    __u8  drop_on_anomaly;
    __u8  _pad[6];
};

/* Map indices */
#define MAP_IDX_STATS      0
#define MAP_IDX_GLOBAL     0
#define MAP_IDX_CONFIG     0

#endif /* __KLAIS_H__ */
