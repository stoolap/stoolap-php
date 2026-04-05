/*
 * stoolap_daemon.h — Daemon protocol, opcodes, shared memory layout
 *
 * Pure C — no PHP/Zend dependencies.
 * This file is included by both the daemon (forked process) and the extension.
 */

#ifndef STOOLAP_DAEMON_H
#define STOOLAP_DAEMON_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* ================================================================
 * Socket path — unique per php-fpm master (pid) so each fpm
 * instance gets its own daemon. Stale sockets are cleaned up
 * automatically when the daemon detects its parent died.
 *
 * stoolap_daemon_init_paths() must be called once from MINIT
 * (in the fpm master process) before any fork happens.
 * ================================================================ */

extern char g_daemon_sock[64];

static inline void stoolap_daemon_init_paths(pid_t master_pid) {
    snprintf(g_daemon_sock, sizeof(g_daemon_sock), "/tmp/stoolap_%d.sock", (int)master_pid);
}

#define STOOLAP_DAEMON_SOCK  g_daemon_sock

/* ================================================================
 * Opcodes (client → daemon)
 * ================================================================ */

#define OP_OPEN             0x00  /* dsn → OK */
#define OP_EXEC             0x01  /* sql → i64 affected */
#define OP_EXEC_PARAMS      0x02  /* sql + params → i64 affected */
#define OP_QUERY            0x03  /* sql → binary buffer */
#define OP_QUERY_PARAMS     0x04  /* sql + params → binary buffer */
#define OP_PREPARE          0x05  /* sql → u32 stmt_id */
#define OP_STMT_EXEC        0x06  /* stmt_id + params → i64 affected */
#define OP_STMT_QUERY       0x07  /* stmt_id + params → binary buffer */
#define OP_STMT_FINALIZE    0x08  /* stmt_id → OK */
#define OP_BEGIN            0x09  /* i32 isolation → u32 tx_id */
#define OP_TX_EXEC          0x0A  /* tx_id + sql → i64 affected */
#define OP_TX_EXEC_PARAMS   0x0B  /* tx_id + sql + params → i64 affected */
#define OP_TX_QUERY         0x0C  /* tx_id + sql → binary buffer */
#define OP_TX_QUERY_PARAMS  0x0D  /* tx_id + sql + params → binary buffer */
#define OP_TX_COMMIT        0x0E  /* tx_id → OK */
#define OP_TX_ROLLBACK      0x0F  /* tx_id → OK */
#define OP_TX_STMT_EXEC     0x10  /* tx_id + stmt_id + params → i64 affected */
#define OP_TX_STMT_QUERY    0x11  /* tx_id + stmt_id + params → binary buffer */
#define OP_EXEC_BATCH       0x12  /* sql + batch_count + params[] → i64 affected */
#define OP_TX_EXEC_BATCH    0x13  /* tx_id + sql + batch_count + params[] → i64 affected */
#define OP_VERSION          0x14  /* → string */
#define OP_CLOSE            0x15  /* → OK, daemon closes connection */

/* ================================================================
 * Response status
 * ================================================================ */

#define RESP_OK     0x00
#define RESP_ERROR  0x01

/* ================================================================
 * Shared memory layout
 *
 * Total: 8MB per connection
 * [0-127]       Control block (stoolap_ctl_t) — 2 cache lines
 * [128 - 4MB)   Request data buffer
 * [4MB - 8MB)   Response data buffer
 *
 * Cache line 0 (bytes 0-63):   client → daemon  (client writes, daemon reads)
 * Cache line 1 (bytes 64-127): daemon → client  (daemon writes, client reads)
 * This eliminates false sharing on the hot atomic flags.
 * ================================================================ */

#define SHM_TOTAL_SIZE    (8 * 1024 * 1024)
#define SHM_CTL_SIZE      128
#define SHM_REQ_OFF       128
#define SHM_REQ_MAX       (4 * 1024 * 1024 - SHM_CTL_SIZE)
#define SHM_RESP_OFF      (4 * 1024 * 1024)
#define SHM_RESP_MAX      (4 * 1024 * 1024)

/* Control block at offset 0 in shared memory.
 * Split across two cache lines to avoid false sharing between client and daemon. */
typedef struct {
    /* --- Cache line 0: client → daemon --- */
    volatile uint32_t req_ready;   /* 0:  1 = request available */
    uint8_t  opcode;               /* 4:  request opcode */
    uint8_t  _cl0_pad[3];          /* 5-7 */
    uint32_t req_len;              /* 8:  bytes written in request buffer */
    uint32_t _cl0_reserved[13];    /* 12-63: pad to 64 bytes */

    /* --- Cache line 1: daemon → client --- */
    volatile uint32_t resp_ready;  /* 64: 1 = response available */
    uint8_t  resp_status;          /* 68: RESP_OK or RESP_ERROR */
    uint8_t  closed;               /* 69: 1 = connection closing */
    uint8_t  _cl1_pad[2];          /* 70-71 */
    uint32_t resp_len;             /* 72: bytes written in response buffer */
    uint32_t _cl1_reserved[13];    /* 76-127: pad to 64 bytes */
} stoolap_ctl_t;

/* Spin hint for ARM (yield) and x86 (pause) */
#if defined(__aarch64__)
#define CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#elif defined(__x86_64__) || defined(__i386__)
#define CPU_PAUSE() __asm__ volatile("pause" ::: "memory")
#else
#define CPU_PAUSE() ((void)0)
#endif

/* Spin iterations before falling back to kernel wait.
 * On ARM (yield ~1ns each): 4000 iters ≈ 4μs spin window.
 * On x86 (pause ~10ns each): 4000 iters ≈ 40μs spin window. */
#define SPIN_ITERS 4000

/* ================================================================
 * Platform-specific shared-memory wait/wake
 *
 * Replaces pipe-based wakeup with single-syscall primitives:
 *   Linux:  futex(FUTEX_WAIT/FUTEX_WAKE)
 *   macOS:  __ulock_wait/__ulock_wake (private but stable API,
 *           used by Go runtime, Rust parking_lot, libdispatch)
 *
 * shm_wait: if *addr == expected, block until woken or timeout.
 *           Returns 0 on wake, <0 on timeout/error.
 * shm_wake: wake one waiter blocked on addr.
 * ================================================================ */

#if defined(__linux__)

#include <time.h>
#include <linux/futex.h>
#include <sys/syscall.h>

static inline int shm_wait(volatile uint32_t *addr, uint32_t expected,
                            uint32_t timeout_us)
{
    struct timespec ts;
    struct timespec *tsp = NULL;
    if (timeout_us > 0) {
        ts.tv_sec  = timeout_us / 1000000;
        ts.tv_nsec = (timeout_us % 1000000) * 1000;
        tsp = &ts;
    }
    return (int)syscall(SYS_futex, addr, FUTEX_WAIT, expected, tsp, NULL, 0);
}

static inline int shm_wake(volatile uint32_t *addr)
{
    return (int)syscall(SYS_futex, addr, FUTEX_WAKE, 1, NULL, NULL, 0);
}

#elif defined(__APPLE__)

/* macOS ulock — kernel wait/wake on shared memory address.
 * Not in public headers but ABI-stable since macOS 10.12.
 * Used by: Go runtime, Rust std/parking_lot, libdispatch, WebKit. */
extern int __ulock_wait(uint32_t op, void *addr, uint64_t value,
                         uint32_t timeout_us);
extern int __ulock_wake(uint32_t op, void *addr, uint64_t wake_value);

#define UL_COMPARE_AND_WAIT_SHARED  3
#define ULF_NO_ERRNO                0x01000000

static inline int shm_wait(volatile uint32_t *addr, uint32_t expected,
                            uint32_t timeout_us)
{
    return __ulock_wait(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO,
                        (void *)addr, (uint64_t)expected, timeout_us);
}

static inline int shm_wake(volatile uint32_t *addr)
{
    return __ulock_wake(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO,
                        (void *)addr, 0);
}

#else
#error "Platform not supported: need futex (Linux) or __ulock (macOS)"
#endif

/* Access macros */
#define SHM_CTL(base)   ((stoolap_ctl_t *)(base))
#define SHM_REQ(base)   ((uint8_t *)(base) + SHM_REQ_OFF)
#define SHM_RESP(base)  ((uint8_t *)(base) + SHM_RESP_OFF)

/* ================================================================
 * Param wire format
 *
 * [u8 type][data]
 * type 0 (null):      no data
 * type 1 (int64):     8 bytes
 * type 2 (float64):   8 bytes
 * type 3 (text):      u32 len + bytes
 * type 4 (bool):      1 byte
 * type 5 (timestamp): 8 bytes (nanos)
 * type 6 (json):      u32 len + bytes
 * type 7 (blob):      u32 len + bytes
 * ================================================================ */

#define PARAM_NULL      0
#define PARAM_INT64     1
#define PARAM_FLOAT64   2
#define PARAM_TEXT      3
#define PARAM_BOOL      4
#define PARAM_TIMESTAMP 5
#define PARAM_JSON      6
#define PARAM_BLOB      7

/* ================================================================
 * Stoolap C API types (only if not already provided by php_stoolap.h)
 * ================================================================ */

#ifndef PHP_STOOLAP_H

typedef struct StoolapDB StoolapDB;
typedef struct StoolapStmt StoolapStmt;
typedef struct StoolapTx StoolapTx;
typedef struct StoolapRows StoolapRows;

typedef struct StoolapValue {
    int32_t value_type;
    int32_t _padding;
    union {
        int64_t  integer;
        double   float64;
        int32_t  boolean;
        struct { const char *ptr; int64_t len; } text;
        struct { const uint8_t *ptr; int64_t len; } blob;
        int64_t  timestamp_nanos;
    } v;
} StoolapValue;

#endif /* PHP_STOOLAP_H */

/* Stoolap FFI — linked directly */
extern const char *stoolap_version(void);
extern int32_t stoolap_open(const char *, StoolapDB **);
extern int32_t stoolap_open_in_memory(StoolapDB **);
extern int32_t stoolap_clone(const StoolapDB *, StoolapDB **);
extern int32_t stoolap_close(StoolapDB *);
extern const char *stoolap_errmsg(const StoolapDB *);
extern int32_t stoolap_exec(StoolapDB *, const char *, int64_t *);
extern int32_t stoolap_exec_params(StoolapDB *, const char *, const StoolapValue *, int32_t, int64_t *);
extern int32_t stoolap_query(StoolapDB *, const char *, StoolapRows **);
extern int32_t stoolap_query_params(StoolapDB *, const char *, const StoolapValue *, int32_t, StoolapRows **);
extern int32_t stoolap_prepare(StoolapDB *, const char *, StoolapStmt **);
extern int32_t stoolap_stmt_exec(StoolapStmt *, const StoolapValue *, int32_t, int64_t *);
extern int32_t stoolap_stmt_query(StoolapStmt *, const StoolapValue *, int32_t, StoolapRows **);
extern void stoolap_stmt_finalize(StoolapStmt *);
extern const char *stoolap_stmt_errmsg(const StoolapStmt *);
extern int32_t stoolap_begin(StoolapDB *, StoolapTx **);
extern int32_t stoolap_begin_with_isolation(StoolapDB *, int32_t, StoolapTx **);
extern int32_t stoolap_tx_exec(StoolapTx *, const char *, int64_t *);
extern int32_t stoolap_tx_exec_params(StoolapTx *, const char *, const StoolapValue *, int32_t, int64_t *);
extern int32_t stoolap_tx_query(StoolapTx *, const char *, StoolapRows **);
extern int32_t stoolap_tx_query_params(StoolapTx *, const char *, const StoolapValue *, int32_t, StoolapRows **);
extern int32_t stoolap_tx_stmt_exec(StoolapTx *, const StoolapStmt *, const StoolapValue *, int32_t, int64_t *);
extern int32_t stoolap_tx_stmt_query(StoolapTx *, const StoolapStmt *, const StoolapValue *, int32_t, StoolapRows **);
extern int32_t stoolap_tx_commit(StoolapTx *);
extern int32_t stoolap_tx_rollback(StoolapTx *);
extern const char *stoolap_tx_errmsg(const StoolapTx *);
extern int32_t stoolap_rows_fetch_all(StoolapRows *, uint8_t **, int64_t *);
extern void stoolap_rows_close(StoolapRows *);
extern const char *stoolap_rows_errmsg(const StoolapRows *);
extern void stoolap_buffer_free(uint8_t *, int64_t);

/* ================================================================
 * Daemon limits
 * ================================================================ */

#define DAEMON_MAX_STMTS   256
#define DAEMON_MAX_TXS     64
#define DAEMON_MAX_PARAMS  1024
#define DAEMON_MAX_DBS     256
#define DAEMON_IDLE_SEC    30
#define DAEMON_POOL_SIZE   8    /* initial worker threads (grows on demand) */
#define DAEMON_POOL_MAX    64   /* max worker threads */
#define DAEMON_POOL_STACK  (256 * 1024) /* 256KB stack per worker (vs 8MB default) */

/* ================================================================
 * Daemon entry point (called after fork(), never returns)
 * Called after fork(), never returns.
 * ================================================================ */

void stoolap_save_argv0(char *argv0);
void stoolap_daemon_set_parent(pid_t pid);
void stoolap_daemon_run(char *argv0, size_t argv0_len);

#endif /* STOOLAP_DAEMON_H */
