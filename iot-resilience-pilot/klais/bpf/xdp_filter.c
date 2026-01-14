/* KLAIS XDP Packet Filter
 *
 * Wire-speed packet filtering using XDP (eXpress Data Path).
 * This program runs at the NIC driver level, before the kernel
 * network stack, enabling sub-microsecond filtering decisions.
 *
 * Compile with:
 *   clang -O2 -target bpf -c xdp_filter.c -o xdp_filter.o
 */

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

/* KLAIS Protocol Constants */
#define KLAIS_MAGIC 0x55AA  /* Network byte order: 0xAA55 */
#define DEVICE_ID_SIZE 16
#define MIN_PACKET_SIZE (2 + 16 + 4)  /* magic + device_id + seq */

/* KLAIS Packet Header */
struct klais_header {
    __u16 magic;
    __u8  device_id[DEVICE_ID_SIZE];
    __u32 sequence;
} __attribute__((packed));

/* Token bucket structure for rate limiting */
struct token_bucket {
    __u64 tokens;
    __u64 last_refill_ns;
    __u64 max_tokens;
    __u64 refill_rate;  /* tokens per second */
};

/* Statistics structure (per-CPU to avoid contention) */
struct stats {
    __u64 received;
    __u64 passed;
    __u64 dropped_magic;
    __u64 dropped_rate;
    __u64 dropped_size;
};

/* Event structure for ring buffer */
struct klais_event {
    __u8  device_id[DEVICE_ID_SIZE];
    __u32 sequence;
    __u32 payload_len;
    __u64 timestamp_ns;
};

/* AI configuration (updated from userspace) */
struct ai_config {
    __u64 global_rate_limit;
    __u64 burst_size;
    __u8  enforce_per_device;
    __u8  drop_on_anomaly;
};

/* ======================== BPF MAPS ======================== */

/* Per-device rate limiting buckets */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 65536);
    __type(key, __u32);  /* FNV-1a hash of device_id */
    __type(value, struct token_bucket);
} device_limits SEC(".maps");

/* Global token bucket */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct token_bucket);
} global_bucket SEC(".maps");

/* Per-CPU statistics */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct stats);
} statistics SEC(".maps");

/* Ring buffer for events to userspace */
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1048576);  /* 1MB */
} events SEC(".maps");

/* AI configuration (updated by control plane) */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct ai_config);
} ai_params SEC(".maps");

/* ======================== HELPER FUNCTIONS ======================== */

/* FNV-1a hash for device ID */
static __always_inline __u32 fnv1a_hash(const __u8 *data, int len) {
    __u32 hash = 2166136261;
    
    #pragma unroll
    for (int i = 0; i < DEVICE_ID_SIZE; i++) {
        hash ^= data[i];
        hash *= 16777619;
    }
    
    return hash;
}

/* Try to consume a token from bucket, returns 1 if successful */
static __always_inline int try_consume_token(struct token_bucket *bucket) {
    __u64 now = bpf_ktime_get_ns();
    __u64 elapsed_ns = now - bucket->last_refill_ns;
    
    /* Refill tokens based on elapsed time */
    if (elapsed_ns > 1000000) {  /* At least 1ms */
        __u64 new_tokens = (elapsed_ns * bucket->refill_rate) / 1000000000ULL;
        if (new_tokens > 0) {
            __u64 updated = bucket->tokens + new_tokens;
            if (updated > bucket->max_tokens) {
                updated = bucket->max_tokens;
            }
            bucket->tokens = updated;
            bucket->last_refill_ns = now;
        }
    }
    
    /* Try to consume */
    if (bucket->tokens > 0) {
        bucket->tokens--;
        return 1;
    }
    
    return 0;
}

/* ======================== XDP PROGRAM ======================== */

SEC("xdp")
int klais_xdp_filter(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;
    
    __u32 key = 0;
    struct stats *stats = bpf_map_lookup_elem(&statistics, &key);
    if (!stats) {
        return XDP_PASS;  /* Should never happen */
    }
    
    stats->received++;
    
    /* 1. Size check */
    if (data + MIN_PACKET_SIZE > data_end) {
        stats->dropped_size++;
        return XDP_DROP;
    }
    
    struct klais_header *hdr = data;
    
    /* 2. Magic validation (0xAA55 in big-endian = 0x55AA in little-endian) */
    if (hdr->magic != bpf_htons(0xAA55)) {
        stats->dropped_magic++;
        return XDP_DROP;
    }
    
    /* 3. Get AI configuration */
    struct ai_config *config = bpf_map_lookup_elem(&ai_params, &key);
    
    /* 4. Per-device rate limiting */
    if (config && config->enforce_per_device) {
        __u32 device_hash = fnv1a_hash(hdr->device_id, DEVICE_ID_SIZE);
        struct token_bucket *device_bucket = bpf_map_lookup_elem(&device_limits, &device_hash);
        
        if (device_bucket && !try_consume_token(device_bucket)) {
            stats->dropped_rate++;
            return XDP_DROP;
        }
    }
    
    /* 5. Global rate limiting */
    struct token_bucket *global = bpf_map_lookup_elem(&global_bucket, &key);
    if (global && !try_consume_token(global)) {
        stats->dropped_rate++;
        return XDP_DROP;
    }
    
    /* 6. Send event to userspace via ring buffer */
    struct klais_event *event = bpf_ringbuf_reserve(&events, sizeof(*event), 0);
    if (event) {
        __builtin_memcpy(event->device_id, hdr->device_id, DEVICE_ID_SIZE);
        event->sequence = bpf_ntohl(hdr->sequence);
        event->payload_len = (data_end - data) - MIN_PACKET_SIZE;
        event->timestamp_ns = bpf_ktime_get_ns();
        bpf_ringbuf_submit(event, 0);
    }
    
    stats->passed++;
    return XDP_PASS;
}

char LICENSE[] SEC("license") = "GPL";
