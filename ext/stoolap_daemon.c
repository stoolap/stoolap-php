/*
 * stoolap_daemon.c — Standalone daemon process for Stoolap PHP extension
 *
 * Pure C — no PHP/Zend dependencies.
 * Forked from the PHP process, serves multiple database connections over
 * shared memory + futex/ulock IPC. Thread pool with elastic scaling.
 */

#include "stoolap_daemon.h"

/* Storage for socket path (declared extern in header) */
char g_daemon_sock[64];

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <time.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif


/* ================================================================
 * Logging
 * ================================================================ */

#define LOG_PREFIX "[stoolap-daemon] "

static void log_err(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void log_err(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, LOG_PREFIX "ERROR: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

static void log_info(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void log_info(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, LOG_PREFIX);
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

#ifdef STOOLAP_DAEMON_DEBUG
static void log_dbg(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void log_dbg(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, LOG_PREFIX "DBG: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}
#define LOG_DBG(...) log_dbg(__VA_ARGS__)
#else
#define LOG_DBG(...) ((void)0)
#endif

#define LOG_ERR(...) log_err(__VA_ARGS__)
#define LOG_INFO(...) log_info(__VA_ARGS__)

/* ================================================================
 * Statement and transaction slots
 * ================================================================ */

typedef struct {
    uint32_t     id;    /* user-facing handle ID, 0 = unused */
    StoolapStmt *stmt;
} stmt_slot_t;

typedef struct {
    uint32_t   id;      /* user-facing handle ID, 0 = unused */
    StoolapTx *tx;
} tx_slot_t;

/* ================================================================
 * Per-connection state
 * ================================================================ */

typedef struct {
    int          client_fd;         /* unix socket (liveness detection via POLLHUP) */
    void        *shm_base;          /* mmap'd shared memory */
    char         shm_name[32];      /* for shm_unlink on cleanup */
    StoolapDB   *db;                /* clone()'d handle for this connection */
    char         dsn[4096];         /* for ref counting */

    /* Pre-allocated param array (no per-request malloc) */
    StoolapValue params[DAEMON_MAX_PARAMS];

    /* Prepared statements */
    stmt_slot_t  stmts[DAEMON_MAX_STMTS];
    uint32_t     stmt_count;
    uint32_t     next_stmt_id;

    /* Transactions */
    tx_slot_t    txs[DAEMON_MAX_TXS];
    uint32_t     tx_count;
    uint32_t     next_tx_id;
} conn_t;

/* ================================================================
 * Database registry
 * ================================================================ */

typedef struct {
    char       dsn[4096];
    StoolapDB *master;
    int        ref_count;
} db_entry_t;

static db_entry_t     g_dbs[DAEMON_MAX_DBS];
static int            g_db_count = 0;
static pthread_mutex_t g_db_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ================================================================
 * Global state
 * ================================================================ */

static volatile sig_atomic_t g_shutdown = 0;
static int                   g_self_pipe[2] = {-1, -1};
static uint32_t              g_next_shm_id = 0;
static pthread_mutex_t       g_shm_id_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Active connection tracking */
static int                   g_conn_count = 0;
static pthread_mutex_t       g_conn_count_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ================================================================
 * Thread pool — pre-created worker threads pick up connections
 * from a queue, avoiding pthread_create/destroy per connection.
 * ================================================================ */

typedef struct {
    pthread_t       threads[DAEMON_POOL_MAX];
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    conn_t         *queue[DAEMON_POOL_MAX * 2]; /* ring buffer */
    int             head;
    int             tail;
    int             count;     /* items in queue */
    int             capacity;  /* queue capacity */
    int             shutdown;
    int             size;      /* threads created */
    int             idle;      /* threads waiting for work */
} thread_pool_t;

static thread_pool_t g_pool;

/* ================================================================
 * Utility: set fd non-blocking
 * ================================================================ */

static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ================================================================
 * Utility: read all bytes
 * ================================================================ */

static int read_all(int fd, void *buf, size_t len)
{
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < len) {
        ssize_t n = read(fd, p + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1; /* EOF */
        off += (size_t)n;
    }
    return 0;
}

/* ================================================================
 * Utility: write all bytes
 * ================================================================ */

static int write_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, p + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

/* ================================================================
 * Database registry operations
 * ================================================================ */

/* Acquire or create a master DB handle for the given DSN.
 * Returns a clone()'d handle for the caller's exclusive use.
 * Must be called with g_db_mutex NOT held. */
static StoolapDB *db_registry_acquire(const char *dsn)
{
    StoolapDB *clone_db = NULL;

    pthread_mutex_lock(&g_db_mutex);

    /* Look for existing entry */
    for (int i = 0; i < g_db_count; i++) {
        if (strcmp(g_dbs[i].dsn, dsn) == 0) {
            g_dbs[i].ref_count++;
            if (stoolap_clone(g_dbs[i].master, &clone_db) != 0) {
                g_dbs[i].ref_count--;
                pthread_mutex_unlock(&g_db_mutex);
                LOG_ERR("stoolap_clone failed for dsn=%s", dsn);
                return NULL;
            }
            pthread_mutex_unlock(&g_db_mutex);
            return clone_db;
        }
    }

    /* Create new entry */
    if (g_db_count >= DAEMON_MAX_DBS) {
        pthread_mutex_unlock(&g_db_mutex);
        LOG_ERR("max databases reached (%d)", DAEMON_MAX_DBS);
        return NULL;
    }

    StoolapDB *master = NULL;
    int32_t rc;

    /* Normalize memory DSN variants */
    if (dsn[0] == '\0' || strcmp(dsn, ":memory:") == 0) {
        rc = stoolap_open("memory://", &master);
    } else {
        rc = stoolap_open(dsn, &master);
    }

    if (rc != 0) {
        pthread_mutex_unlock(&g_db_mutex);
        LOG_ERR("stoolap_open failed for dsn=%s", dsn);
        return NULL;
    }

    if (stoolap_clone(master, &clone_db) != 0) {
        stoolap_close(master);
        pthread_mutex_unlock(&g_db_mutex);
        LOG_ERR("stoolap_clone failed for dsn=%s", dsn);
        return NULL;
    }

    db_entry_t *e = &g_dbs[g_db_count];
    strncpy(e->dsn, dsn, sizeof(e->dsn) - 1);
    e->dsn[sizeof(e->dsn) - 1] = '\0';
    e->master = master;
    e->ref_count = 1;
    g_db_count++;

    pthread_mutex_unlock(&g_db_mutex);
    return clone_db;
}

/* Release a reference. If ref_count drops to 0, close the master handle. */
static void db_registry_release(const char *dsn)
{
    pthread_mutex_lock(&g_db_mutex);

    for (int i = 0; i < g_db_count; i++) {
        if (strcmp(g_dbs[i].dsn, dsn) == 0) {
            g_dbs[i].ref_count--;
            /* Don't close master when ref_count hits 0 — keep it alive
             * so reconnecting clients find the same data. Masters are
             * only closed on daemon shutdown. */
            break;
        }
    }

    pthread_mutex_unlock(&g_db_mutex);
}

/* Close all remaining master handles (during shutdown). */
static void db_registry_close_all(void)
{
    pthread_mutex_lock(&g_db_mutex);
    for (int i = 0; i < g_db_count; i++) {
        stoolap_close(g_dbs[i].master);
    }
    g_db_count = 0;
    pthread_mutex_unlock(&g_db_mutex);
}

/* ================================================================
 * Shared memory helpers
 * ================================================================ */

/* Generate a unique shm name. macOS limits to 31 chars. */
static void make_shm_name(char *buf, size_t buflen)
{
    uint32_t id;
    pthread_mutex_lock(&g_shm_id_mutex);
    id = g_next_shm_id++;
    pthread_mutex_unlock(&g_shm_id_mutex);

    snprintf(buf, buflen, "/stlp_%d_%u", (int)getpid(), id);
}

/* Create and mmap a shared memory segment. Returns the base pointer or NULL. */
static void *shm_create(const char *name)
{
    int fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, 0600);
    if (fd < 0) {
        LOG_ERR("shm_open(%s) failed: %s", name, strerror(errno));
        return NULL;
    }

    if (ftruncate(fd, SHM_TOTAL_SIZE) < 0) {
        LOG_ERR("ftruncate(%s) failed: %s", name, strerror(errno));
        close(fd);
        shm_unlink(name);
        return NULL;
    }

    void *base = mmap(NULL, SHM_TOTAL_SIZE, PROT_READ | PROT_WRITE,
                       MAP_SHARED, fd, 0);
    close(fd); /* mmap holds the reference */

    if (base == MAP_FAILED) {
        LOG_ERR("mmap(%s) failed: %s", name, strerror(errno));
        shm_unlink(name);
        return NULL;
    }

    /* Zero out the control block */
    memset(base, 0, SHM_CTL_SIZE);

    return base;
}

/* SCM_RIGHTS removed — futex/ulock replaces pipe-based wakeup,
 * no file descriptors need to be passed to clients. */

/* ================================================================
 * Buffer reading helpers (request buffer, little-endian)
 * ================================================================ */

static inline uint8_t rd_u8(const uint8_t *p) { return *p; }

static inline uint32_t rd_u32(const uint8_t *p) {
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static inline int32_t rd_i32(const uint8_t *p) {
    int32_t v;
    memcpy(&v, p, 4);
    return v;
}

static inline int64_t rd_i64(const uint8_t *p) {
    int64_t v;
    memcpy(&v, p, 8);
    return v;
}

static inline double rd_f64(const uint8_t *p) {
    double v;
    memcpy(&v, p, 8);
    return v;
}

/* ================================================================
 * Buffer writing helpers (response buffer, little-endian)
 * ================================================================ */

static inline void wr_u32(uint8_t *p, uint32_t v) {
    memcpy(p, &v, 4);
}

static inline void wr_i64(uint8_t *p, int64_t v) {
    memcpy(p, &v, 8);
}

/* ================================================================
 * Response writers — write directly to SHM_RESP
 * ================================================================ */

static void write_error(conn_t *c, const char *msg)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);
    uint8_t *resp = SHM_RESP(c->shm_base);
    size_t len = msg ? strlen(msg) : 0;
    if (len > SHM_RESP_MAX - 4) {
        len = SHM_RESP_MAX - 4;
    }
    wr_u32(resp, (uint32_t)len);
    if (len > 0) {
        memcpy(resp + 4, msg, len);
    }
    ctl->resp_status = RESP_ERROR;
    ctl->resp_len = (uint32_t)(4 + len);
}

static void write_ok_empty(conn_t *c)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);
    ctl->resp_status = RESP_OK;
    ctl->resp_len = 0;
}

static void write_ok_i64(conn_t *c, int64_t val)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);
    uint8_t *resp = SHM_RESP(c->shm_base);
    wr_i64(resp, val);
    ctl->resp_status = RESP_OK;
    ctl->resp_len = 8;
}

static void write_ok_u32(conn_t *c, uint32_t val)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);
    uint8_t *resp = SHM_RESP(c->shm_base);
    wr_u32(resp, val);
    ctl->resp_status = RESP_OK;
    ctl->resp_len = 4;
}

static void write_ok_string(conn_t *c, const char *str)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);
    uint8_t *resp = SHM_RESP(c->shm_base);
    size_t len = str ? strlen(str) : 0;
    if (len > SHM_RESP_MAX - 4) {
        len = SHM_RESP_MAX - 4;
    }
    wr_u32(resp, (uint32_t)len);
    if (len > 0) {
        memcpy(resp + 4, str, len);
    }
    ctl->resp_status = RESP_OK;
    ctl->resp_len = (uint32_t)(4 + len);
}

static void write_ok_buffer(conn_t *c, const uint8_t *buf, int64_t buf_len)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);
    uint8_t *resp = SHM_RESP(c->shm_base);

    if ((size_t)buf_len > SHM_RESP_MAX) {
        write_error(c, "response too large for shared memory buffer");
        return;
    }

    memcpy(resp, buf, (size_t)buf_len);
    ctl->resp_status = RESP_OK;
    ctl->resp_len = (uint32_t)buf_len;
}

/* ================================================================
 * Param deserialization — read from SHM_REQ into conn->params[]
 *
 * Returns the number of params read, or -1 on error.
 * *offset is advanced past the consumed bytes.
 * Text/json/blob ptr fields point directly into shm (zero-copy).
 * ================================================================ */

static int deserialize_params(conn_t *c, const uint8_t *buf, uint32_t buf_len,
                               uint32_t *offset, uint32_t count)
{
    if (count > DAEMON_MAX_PARAMS) {
        return -1;
    }

    uint32_t off = *offset;

    for (uint32_t i = 0; i < count; i++) {
        if (off >= buf_len) return -1;

        uint8_t ptype = rd_u8(buf + off);
        off++;

        StoolapValue *v = &c->params[i];
        memset(v, 0, sizeof(StoolapValue));

        switch (ptype) {
        case PARAM_NULL:
            v->value_type = 0; /* NULL */
            break;

        case PARAM_INT64:
            if (off + 8 > buf_len) return -1;
            v->value_type = 1;
            v->v.integer = rd_i64(buf + off);
            off += 8;
            break;

        case PARAM_FLOAT64:
            if (off + 8 > buf_len) return -1;
            v->value_type = 2;
            v->v.float64 = rd_f64(buf + off);
            off += 8;
            break;

        case PARAM_TEXT:
            if (off + 4 > buf_len) return -1;
            {
                uint32_t slen = rd_u32(buf + off);
                off += 4;
                if (off + slen > buf_len) return -1;
                v->value_type = 3;
                v->v.text.ptr = (const char *)(buf + off);
                v->v.text.len = (int64_t)slen;
                off += slen;
            }
            break;

        case PARAM_BOOL:
            if (off + 1 > buf_len) return -1;
            v->value_type = 4;
            v->v.boolean = (int32_t)rd_u8(buf + off);
            off++;
            break;

        case PARAM_TIMESTAMP:
            if (off + 8 > buf_len) return -1;
            v->value_type = 5;
            v->v.timestamp_nanos = rd_i64(buf + off);
            off += 8;
            break;

        case PARAM_JSON:
            if (off + 4 > buf_len) return -1;
            {
                uint32_t slen = rd_u32(buf + off);
                off += 4;
                if (off + slen > buf_len) return -1;
                v->value_type = 6;
                v->v.text.ptr = (const char *)(buf + off);
                v->v.text.len = (int64_t)slen;
                off += slen;
            }
            break;

        case PARAM_BLOB:
            if (off + 4 > buf_len) return -1;
            {
                uint32_t slen = rd_u32(buf + off);
                off += 4;
                if (off + slen > buf_len) return -1;
                v->value_type = 7;
                v->v.blob.ptr = buf + off;
                v->v.blob.len = (int64_t)slen;
                off += slen;
            }
            break;

        default:
            return -1;
        }
    }

    *offset = off;
    return (int)count;
}

/* ================================================================
 * Statement slot management
 * ================================================================ */

static stmt_slot_t *stmt_alloc(conn_t *c, StoolapStmt *stmt, uint32_t *out_id)
{
    if (c->stmt_count >= DAEMON_MAX_STMTS) return NULL;

    for (uint32_t i = 0; i < DAEMON_MAX_STMTS; i++) {
        if (c->stmts[i].id == 0) {
            c->next_stmt_id++;
            if (c->next_stmt_id == 0) c->next_stmt_id = 1; /* skip 0 */
            c->stmts[i].id   = c->next_stmt_id;
            c->stmts[i].stmt = stmt;
            c->stmt_count++;
            *out_id = c->stmts[i].id;
            return &c->stmts[i];
        }
    }
    return NULL;
}

static StoolapStmt *stmt_find(conn_t *c, uint32_t id)
{
    if (id == 0) return NULL;
    for (uint32_t i = 0; i < DAEMON_MAX_STMTS; i++) {
        if (c->stmts[i].id == id) {
            return c->stmts[i].stmt;
        }
    }
    return NULL;
}

static int stmt_remove(conn_t *c, uint32_t id)
{
    if (id == 0) return -1;
    for (uint32_t i = 0; i < DAEMON_MAX_STMTS; i++) {
        if (c->stmts[i].id == id) {
            stoolap_stmt_finalize(c->stmts[i].stmt);
            c->stmts[i].id   = 0;
            c->stmts[i].stmt = NULL;
            c->stmt_count--;
            return 0;
        }
    }
    return -1;
}

static void stmt_free_all(conn_t *c)
{
    for (uint32_t i = 0; i < DAEMON_MAX_STMTS; i++) {
        if (c->stmts[i].id != 0) {
            stoolap_stmt_finalize(c->stmts[i].stmt);
            c->stmts[i].id   = 0;
            c->stmts[i].stmt = NULL;
        }
    }
    c->stmt_count = 0;
}

/* ================================================================
 * Transaction slot management
 * ================================================================ */

static tx_slot_t *tx_alloc(conn_t *c, StoolapTx *tx, uint32_t *out_id)
{
    if (c->tx_count >= DAEMON_MAX_TXS) return NULL;

    for (uint32_t i = 0; i < DAEMON_MAX_TXS; i++) {
        if (c->txs[i].id == 0) {
            c->next_tx_id++;
            if (c->next_tx_id == 0) c->next_tx_id = 1;
            c->txs[i].id = c->next_tx_id;
            c->txs[i].tx = tx;
            c->tx_count++;
            *out_id = c->txs[i].id;
            return &c->txs[i];
        }
    }
    return NULL;
}

static StoolapTx *tx_find(conn_t *c, uint32_t id)
{
    if (id == 0) return NULL;
    for (uint32_t i = 0; i < DAEMON_MAX_TXS; i++) {
        if (c->txs[i].id == id) {
            return c->txs[i].tx;
        }
    }
    return NULL;
}

static int tx_remove(conn_t *c, uint32_t id)
{
    if (id == 0) return -1;
    for (uint32_t i = 0; i < DAEMON_MAX_TXS; i++) {
        if (c->txs[i].id == id) {
            /* Note: caller must have already committed/rolled back */
            c->txs[i].id = 0;
            c->txs[i].tx = NULL;
            c->tx_count--;
            return 0;
        }
    }
    return -1;
}

static void tx_rollback_all(conn_t *c)
{
    for (uint32_t i = 0; i < DAEMON_MAX_TXS; i++) {
        if (c->txs[i].id != 0 && c->txs[i].tx != NULL) {
            stoolap_tx_rollback(c->txs[i].tx);
            c->txs[i].id = 0;
            c->txs[i].tx = NULL;
        }
    }
    c->tx_count = 0;
}

/* ================================================================
 * Query helper — fetch all rows and write buffer to SHM_RESP
 * ================================================================ */

static void fetch_rows_to_response(conn_t *c, StoolapRows *rows)
{
    uint8_t *buf = NULL;
    int64_t  buf_len = 0;

    int32_t rc = stoolap_rows_fetch_all(rows, &buf, &buf_len);
    if (rc != 0) {
        const char *err = stoolap_rows_errmsg(rows);
        stoolap_rows_close(rows);
        write_error(c, err ? err : "rows fetch failed");
        return;
    }

    stoolap_rows_close(rows);

    if (buf && buf_len > 0) {
        write_ok_buffer(c, buf, buf_len);
        stoolap_buffer_free(buf, buf_len);
    } else {
        /* Empty result set — write zero-length OK response */
        write_ok_buffer(c, (const uint8_t *)"", 0);
    }
}

/* ================================================================
 * Request handler — big opcode switch
 * ================================================================ */

/* Read a length-prefixed SQL string from the request buffer.
 * Writes a null-terminator in-place (shm is writable).
 * Returns the SQL pointer, advances *off past the string. */
/* Read length-prefixed SQL from shm, ensure null-termination in-place */
static const char *read_sql(const uint8_t *req, uint32_t req_len, uint32_t *off)
{
    if (*off + 4 > req_len) return NULL;
    uint32_t sql_len = rd_u32(req + *off); *off += 4;
    if (*off + sql_len > req_len) return NULL;
    const char *sql = (const char *)(req + *off);
    *off += sql_len;
    /* Null-terminate in mutable shm. Guard against writing past request buffer. */
    if (sql_len > 0 && sql[sql_len - 1] != '\0' && *off < req_len) {
        ((uint8_t *)req)[*off] = '\0';
    }
    return sql;
}

static void handle_request(conn_t *c)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);
    const uint8_t *req = SHM_REQ(c->shm_base);
    uint8_t opcode     = ctl->opcode;
    uint32_t req_len   = ctl->req_len;
    uint32_t off       = 0;

    switch (opcode) {

    /* OP_EXEC: [u32 sql_len][sql] → i64 affected */
    case OP_EXEC: {
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_EXEC: missing SQL");
            break;
        }
        int64_t affected = 0;
        int32_t rc = stoolap_exec(c->db, sql, &affected);
        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
        } else {
            write_ok_i64(c, affected);
        }
        break;
    }

    /* OP_EXEC_PARAMS: u32 sql_len + sql + u32 param_count + params → i64 affected */
    case OP_EXEC_PARAMS: {
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_EXEC_PARAMS: bad SQL");
            break;
        }

        if (off + 4 > req_len) {
            write_error(c, "OP_EXEC_PARAMS: missing param count");
            break;
        }
        uint32_t pcount = rd_u32(req + off); off += 4;

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_EXEC_PARAMS: bad params");
                break;
            }
        }

        int64_t affected = 0;
        int32_t rc = stoolap_exec_params(c->db, sql, c->params, (int32_t)pcount, &affected);
        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
        } else {
            write_ok_i64(c, affected);
        }
        break;
    }

    /* OP_QUERY: sql → binary buffer */
    case OP_QUERY: {
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_QUERY: missing SQL");
            break;
        }
        StoolapRows *rows = NULL;
        int32_t rc = stoolap_query(c->db, sql, &rows);
        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
        } else {
            fetch_rows_to_response(c, rows);
        }
        break;
    }

    /* OP_QUERY_PARAMS: u32 sql_len + sql + u32 param_count + params → binary buffer */
    case OP_QUERY_PARAMS: {
        if (req_len < 4) {
            write_error(c, "OP_QUERY_PARAMS: truncated");
            break;
        }
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_QUERY_PARAMS: bad SQL");
            break;
        }

        if (off + 4 > req_len) {
            write_error(c, "OP_QUERY_PARAMS: missing param count");
            break;
        }
        uint32_t pcount = rd_u32(req + off); off += 4;

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_QUERY_PARAMS: bad params");
                break;
            }
        }

        StoolapRows *rows = NULL;
        int32_t rc = stoolap_query_params(c->db, sql, c->params, (int32_t)pcount, &rows);
        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
        } else {
            fetch_rows_to_response(c, rows);
        }
        break;
    }

    /* OP_PREPARE: sql → u32 stmt_id */
    case OP_PREPARE: {
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_PREPARE: missing SQL");
            break;
        }
        StoolapStmt *stmt = NULL;
        int32_t rc = stoolap_prepare(c->db, sql, &stmt);
        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
            break;
        }
        uint32_t sid = 0;
        if (!stmt_alloc(c, stmt, &sid)) {
            stoolap_stmt_finalize(stmt);
            write_error(c, "too many prepared statements");
            break;
        }
        write_ok_u32(c, sid);
        break;
    }

    /* OP_STMT_EXEC: u32 stmt_id + u32 param_count + params → i64 affected */
    case OP_STMT_EXEC: {
        if (req_len < 8) {
            write_error(c, "OP_STMT_EXEC: truncated");
            break;
        }
        uint32_t sid = rd_u32(req + off); off += 4;
        uint32_t pcount = rd_u32(req + off); off += 4;

        StoolapStmt *stmt = stmt_find(c, sid);
        if (!stmt) {
            write_error(c, "OP_STMT_EXEC: invalid stmt_id");
            break;
        }

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_STMT_EXEC: bad params");
                break;
            }
        }

        int64_t affected = 0;
        int32_t rc = stoolap_stmt_exec(stmt, c->params, (int32_t)pcount, &affected);
        if (rc != 0) {
            write_error(c, stoolap_stmt_errmsg(stmt));
        } else {
            write_ok_i64(c, affected);
        }
        break;
    }

    /* OP_STMT_QUERY: u32 stmt_id + u32 param_count + params → binary buffer */
    case OP_STMT_QUERY: {
        if (req_len < 8) {
            write_error(c, "OP_STMT_QUERY: truncated");
            break;
        }
        uint32_t sid = rd_u32(req + off); off += 4;
        uint32_t pcount = rd_u32(req + off); off += 4;

        StoolapStmt *stmt = stmt_find(c, sid);
        if (!stmt) {
            write_error(c, "OP_STMT_QUERY: invalid stmt_id");
            break;
        }

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_STMT_QUERY: bad params");
                break;
            }
        }

        StoolapRows *rows = NULL;
        int32_t rc = stoolap_stmt_query(stmt, c->params, (int32_t)pcount, &rows);
        if (rc != 0) {
            write_error(c, stoolap_stmt_errmsg(stmt));
        } else {
            fetch_rows_to_response(c, rows);
        }
        break;
    }

    /* OP_STMT_FINALIZE: u32 stmt_id → OK */
    case OP_STMT_FINALIZE: {
        if (req_len < 4) {
            write_error(c, "OP_STMT_FINALIZE: truncated");
            break;
        }
        uint32_t sid = rd_u32(req + off);
        if (stmt_remove(c, sid) != 0) {
            write_error(c, "OP_STMT_FINALIZE: invalid stmt_id");
        } else {
            write_ok_empty(c);
        }
        break;
    }

    /* OP_BEGIN: i32 isolation → u32 tx_id */
    case OP_BEGIN: {
        if (req_len < 4) {
            write_error(c, "OP_BEGIN: truncated");
            break;
        }
        int32_t isolation = rd_i32(req + off);
        StoolapTx *tx = NULL;
        int32_t rc;

        if (isolation < 0) {
            rc = stoolap_begin(c->db, &tx);
        } else {
            rc = stoolap_begin_with_isolation(c->db, isolation, &tx);
        }

        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
            break;
        }

        uint32_t tid = 0;
        if (!tx_alloc(c, tx, &tid)) {
            stoolap_tx_rollback(tx);
            write_error(c, "too many active transactions");
            break;
        }
        write_ok_u32(c, tid);
        break;
    }

    /* OP_TX_EXEC: u32 tx_id + sql → i64 affected */
    case OP_TX_EXEC: {
        if (req_len < 4) {
            write_error(c, "OP_TX_EXEC: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off); off += 4;
        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_EXEC: invalid tx_id");
            break;
        }
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_TX_EXEC: missing SQL");
            break;
        }

        int64_t affected = 0;
        int32_t rc = stoolap_tx_exec(tx, sql, &affected);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
        } else {
            write_ok_i64(c, affected);
        }
        break;
    }

    /* OP_TX_EXEC_PARAMS: u32 tx_id + u32 sql_len + sql + u32 param_count + params → i64 affected */
    case OP_TX_EXEC_PARAMS: {
        if (req_len < 8) {
            write_error(c, "OP_TX_EXEC_PARAMS: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off); off += 4;
        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_EXEC_PARAMS: invalid tx_id");
            break;
        }

        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_TX_EXEC_PARAMS: bad SQL");
            break;
        }

        if (off + 4 > req_len) {
            write_error(c, "OP_TX_EXEC_PARAMS: missing param count");
            break;
        }
        uint32_t pcount = rd_u32(req + off); off += 4;

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_TX_EXEC_PARAMS: bad params");
                break;
            }
        }

        int64_t affected = 0;
        int32_t rc = stoolap_tx_exec_params(tx, sql, c->params, (int32_t)pcount, &affected);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
        } else {
            write_ok_i64(c, affected);
        }
        break;
    }

    /* OP_TX_QUERY: u32 tx_id + sql → binary buffer */
    case OP_TX_QUERY: {
        if (req_len < 4) {
            write_error(c, "OP_TX_QUERY: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off); off += 4;
        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_QUERY: invalid tx_id");
            break;
        }
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_TX_QUERY: missing SQL");
            break;
        }

        StoolapRows *rows = NULL;
        int32_t rc = stoolap_tx_query(tx, sql, &rows);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
        } else {
            fetch_rows_to_response(c, rows);
        }
        break;
    }

    /* OP_TX_QUERY_PARAMS: u32 tx_id + u32 sql_len + sql + u32 param_count + params → binary buffer */
    case OP_TX_QUERY_PARAMS: {
        if (req_len < 8) {
            write_error(c, "OP_TX_QUERY_PARAMS: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off); off += 4;
        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_QUERY_PARAMS: invalid tx_id");
            break;
        }

        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_TX_QUERY_PARAMS: bad SQL");
            break;
        }

        if (off + 4 > req_len) {
            write_error(c, "OP_TX_QUERY_PARAMS: missing param count");
            break;
        }
        uint32_t pcount = rd_u32(req + off); off += 4;

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_TX_QUERY_PARAMS: bad params");
                break;
            }
        }

        StoolapRows *rows = NULL;
        int32_t rc = stoolap_tx_query_params(tx, sql, c->params, (int32_t)pcount, &rows);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
        } else {
            fetch_rows_to_response(c, rows);
        }
        break;
    }

    /* OP_TX_COMMIT: u32 tx_id → OK */
    case OP_TX_COMMIT: {
        if (req_len < 4) {
            write_error(c, "OP_TX_COMMIT: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off);
        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_COMMIT: invalid tx_id");
            break;
        }
        int32_t rc = stoolap_tx_commit(tx);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
            /* Remove slot even on error — tx is consumed */
        }
        tx_remove(c, tid);
        if (rc == 0) {
            write_ok_empty(c);
        }
        break;
    }

    /* OP_TX_ROLLBACK: u32 tx_id → OK */
    case OP_TX_ROLLBACK: {
        if (req_len < 4) {
            write_error(c, "OP_TX_ROLLBACK: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off);
        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_ROLLBACK: invalid tx_id");
            break;
        }
        int32_t rc = stoolap_tx_rollback(tx);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
        }
        tx_remove(c, tid);
        if (rc == 0) {
            write_ok_empty(c);
        }
        break;
    }

    /* OP_TX_STMT_EXEC: u32 tx_id + u32 stmt_id + u32 param_count + params → i64 affected */
    case OP_TX_STMT_EXEC: {
        if (req_len < 12) {
            write_error(c, "OP_TX_STMT_EXEC: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off); off += 4;
        uint32_t sid = rd_u32(req + off); off += 4;
        uint32_t pcount = rd_u32(req + off); off += 4;

        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_STMT_EXEC: invalid tx_id");
            break;
        }
        StoolapStmt *stmt = stmt_find(c, sid);
        if (!stmt) {
            write_error(c, "OP_TX_STMT_EXEC: invalid stmt_id");
            break;
        }

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_TX_STMT_EXEC: bad params");
                break;
            }
        }

        int64_t affected = 0;
        int32_t rc = stoolap_tx_stmt_exec(tx, stmt, c->params, (int32_t)pcount, &affected);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
        } else {
            write_ok_i64(c, affected);
        }
        break;
    }

    /* OP_TX_STMT_QUERY: u32 tx_id + u32 stmt_id + u32 param_count + params → binary buffer */
    case OP_TX_STMT_QUERY: {
        if (req_len < 12) {
            write_error(c, "OP_TX_STMT_QUERY: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off); off += 4;
        uint32_t sid = rd_u32(req + off); off += 4;
        uint32_t pcount = rd_u32(req + off); off += 4;

        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_STMT_QUERY: invalid tx_id");
            break;
        }
        StoolapStmt *stmt = stmt_find(c, sid);
        if (!stmt) {
            write_error(c, "OP_TX_STMT_QUERY: invalid stmt_id");
            break;
        }

        if (pcount > 0) {
            if (deserialize_params(c, req, req_len, &off, pcount) < 0) {
                write_error(c, "OP_TX_STMT_QUERY: bad params");
                break;
            }
        }

        StoolapRows *rows = NULL;
        int32_t rc = stoolap_tx_stmt_query(tx, stmt, c->params, (int32_t)pcount, &rows);
        if (rc != 0) {
            write_error(c, stoolap_tx_errmsg(tx));
        } else {
            fetch_rows_to_response(c, rows);
        }
        break;
    }

    /* OP_EXEC_BATCH: u32 sql_len + sql + u32 param_count_per_row + u32 batch_count + params[] → i64 affected */
    case OP_EXEC_BATCH: {
        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_EXEC_BATCH: missing SQL");
            break;
        }

        if (off + 8 > req_len) {
            write_error(c, "OP_EXEC_BATCH: missing counts");
            break;
        }
        uint32_t params_per_row = rd_u32(req + off); off += 4;
        uint32_t batch_count    = rd_u32(req + off); off += 4;

        /* Begin transaction, prepare once, execute N times, commit */
        StoolapTx *batch_tx = NULL;
        int32_t rc = stoolap_begin(c->db, &batch_tx);
        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
            break;
        }

        StoolapStmt *stmt = NULL;
        rc = stoolap_prepare(c->db, sql, &stmt);
        if (rc != 0) {
            stoolap_tx_rollback(batch_tx);
            write_error(c, stoolap_errmsg(c->db));
            break;
        }

        int64_t total_affected = 0;
        int failed = 0;

        for (uint32_t b = 0; b < batch_count; b++) {
            if (params_per_row > 0) {
                if (deserialize_params(c, req, req_len, &off, params_per_row) < 0) {
                    write_error(c, "OP_EXEC_BATCH: bad params");
                    failed = 1;
                    break;
                }
            }
            int64_t affected = 0;
            rc = stoolap_tx_stmt_exec(batch_tx, stmt, c->params, (int32_t)params_per_row, &affected);
            if (rc != 0) {
                const char *err = stoolap_tx_errmsg(batch_tx);
                write_error(c, err ? err : "OP_EXEC_BATCH: exec failed");
                failed = 1;
                break;
            }
            total_affected += affected;
        }

        stoolap_stmt_finalize(stmt);

        if (failed) {
            stoolap_tx_rollback(batch_tx);
        } else {
            rc = stoolap_tx_commit(batch_tx);
            if (rc != 0) {
                write_error(c, stoolap_errmsg(c->db));
            } else {
                write_ok_i64(c, total_affected);
            }
        }
        break;
    }

    /* OP_TX_EXEC_BATCH: u32 tx_id + u32 sql_len + sql + u32 params_per_row + u32 batch_count + params[] → i64 affected */
    case OP_TX_EXEC_BATCH: {
        if (req_len < 8) {
            write_error(c, "OP_TX_EXEC_BATCH: truncated");
            break;
        }
        uint32_t tid = rd_u32(req + off); off += 4;
        StoolapTx *tx = tx_find(c, tid);
        if (!tx) {
            write_error(c, "OP_TX_EXEC_BATCH: invalid tx_id");
            break;
        }

        const char *sql = read_sql(req, req_len, &off);
        if (!sql) {
            write_error(c, "OP_TX_EXEC_BATCH: missing SQL");
            break;
        }

        if (off + 8 > req_len) {
            write_error(c, "OP_TX_EXEC_BATCH: missing counts");
            break;
        }
        uint32_t params_per_row = rd_u32(req + off); off += 4;
        uint32_t batch_count    = rd_u32(req + off); off += 4;

        /* Prepare once, execute N times within the transaction */
        StoolapStmt *stmt = NULL;
        int32_t rc = stoolap_prepare(c->db, sql, &stmt);
        if (rc != 0) {
            write_error(c, stoolap_errmsg(c->db));
            break;
        }

        int64_t total_affected = 0;
        int failed = 0;

        for (uint32_t b = 0; b < batch_count; b++) {
            if (params_per_row > 0) {
                if (deserialize_params(c, req, req_len, &off, params_per_row) < 0) {
                    write_error(c, "OP_TX_EXEC_BATCH: executeBatch: parameter count mismatch (all rows must have the same number of parameters)");
                    failed = 1;
                    break;
                }
            }

            int64_t affected = 0;
            rc = stoolap_tx_stmt_exec(tx, stmt, c->params, (int32_t)params_per_row, &affected);
            if (rc != 0) {
                write_error(c, stoolap_tx_errmsg(tx));
                failed = 1;
                break;
            }
            total_affected += affected;
        }

        stoolap_stmt_finalize(stmt);

        if (!failed) {
            write_ok_i64(c, total_affected);
        }
        break;
    }

    /* OP_VERSION: → string */
    case OP_VERSION: {
        const char *ver = stoolap_version();
        write_ok_string(c, ver);
        break;
    }

    /* OP_CLOSE: → OK, then thread exits */
    case OP_CLOSE: {
        write_ok_empty(c);
        SHM_CTL(c->shm_base)->closed = 1;
        break;
    }

    /* OP_OPEN: should not arrive here (handled in accept loop) */
    case OP_OPEN: {
        write_ok_empty(c);
        break;
    }

    default:
        write_error(c, "unknown opcode");
        break;
    }
}

/* ================================================================
 * Connection cleanup
 * ================================================================ */

static void conn_cleanup(conn_t *c)
{
    LOG_DBG("cleaning up connection shm=%s", c->shm_name);

    /* Rollback any open transactions */
    tx_rollback_all(c);

    /* Finalize all prepared statements */
    stmt_free_all(c);

    /* Close cloned DB handle */
    if (c->db) {
        stoolap_close(c->db);
        c->db = NULL;
    }

    /* Release DB registry reference */
    if (c->dsn[0] != '\0') {
        db_registry_release(c->dsn);
    }

    /* Unmap and unlink shared memory */
    if (c->shm_base && c->shm_base != MAP_FAILED) {
        munmap(c->shm_base, SHM_TOTAL_SIZE);
        c->shm_base = NULL;
    }
    if (c->shm_name[0] != '\0') {
        shm_unlink(c->shm_name);
        c->shm_name[0] = '\0';
    }

    /* Close the client socket (triggers POLLHUP on client side too) */
    if (c->client_fd >= 0) {
        close(c->client_fd);
        c->client_fd = -1;
    }

    /* Decrement global connection count */
    pthread_mutex_lock(&g_conn_count_mutex);
    g_conn_count--;
    int count = g_conn_count;
    pthread_mutex_unlock(&g_conn_count_mutex);

    LOG_DBG("connection closed, %d remaining", count);

    /* Wake accept loop so it can check idle timeout */
    if (count == 0) {
        uint8_t wake = 1;
        (void)write(g_self_pipe[1], &wake, 1);
    }
}

/* ================================================================
 * Connection thread main
 * ================================================================ */

/* Serve a single connection until it closes or client dies.
 * Called by pool worker threads. */
static void conn_serve(conn_t *c)
{
    stoolap_ctl_t *ctl = SHM_CTL(c->shm_base);

    for (;;) {
        /* Phase 1: spin on atomic req_ready */
        int got_req = 0;
        for (int i = 0; i < SPIN_ITERS; i++) {
            if (__atomic_load_n(&ctl->req_ready, __ATOMIC_ACQUIRE)) {
                got_req = 1;
                break;
            }
            CPU_PAUSE();
        }

        if (!got_req) {
            /* Phase 2: kernel wait (futex/ulock) */
            for (;;) {
                shm_wait(&ctl->req_ready, 0, 5000000); /* 5s timeout */
                if (__atomic_load_n(&ctl->req_ready, __ATOMIC_ACQUIRE)) {
                    got_req = 1;
                    break;
                }
                /* Timeout — check if client is still alive */
                struct pollfd pfd = { .fd = c->client_fd, .events = 0 };
                if (poll(&pfd, 1, 0) > 0 &&
                    (pfd.revents & (POLLHUP | POLLERR | POLLNVAL))) {
                    return;
                }
            }
        }

        __atomic_store_n(&ctl->req_ready, 0, __ATOMIC_RELEASE);
        handle_request(c);

        __atomic_store_n(&ctl->resp_ready, 1, __ATOMIC_RELEASE);
        shm_wake(&ctl->resp_ready);

        if (ctl->closed) break;
    }
}

/* Pool worker thread — waits for connections, serves them, loops back. */
static void *pool_worker(void *arg)
{
    (void)arg;

#if defined(__APPLE__)
    pthread_setname_np("stoolap: worker");
#elif defined(__linux__)
    pthread_setname_np(pthread_self(), "stoolap: worker");
#endif

    for (;;) {
        pthread_mutex_lock(&g_pool.mutex);
        g_pool.idle++;
        while (g_pool.count == 0 && !g_pool.shutdown) {
            pthread_cond_wait(&g_pool.cond, &g_pool.mutex);
        }
        g_pool.idle--;
        if (g_pool.shutdown && g_pool.count == 0) {
            pthread_mutex_unlock(&g_pool.mutex);
            break;
        }
        conn_t *c = g_pool.queue[g_pool.head];
        g_pool.head = (g_pool.head + 1) % g_pool.capacity;
        g_pool.count--;
        pthread_mutex_unlock(&g_pool.mutex);

        LOG_DBG("pool worker serving shm=%s", c->shm_name);
        conn_serve(c);
        conn_cleanup(c);
        free(c);
    }

    return NULL;
}

/* Create one pool thread with reduced stack size. Returns 0 on success. */
static int pool_create_thread(void)
{
    if (g_pool.size >= DAEMON_POOL_MAX) return -1;

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, DAEMON_POOL_STACK);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    int rc = pthread_create(&g_pool.threads[g_pool.size], &attr, pool_worker, NULL);
    pthread_attr_destroy(&attr);
    if (rc != 0) return -1;
    g_pool.size++;
    return 0;
}

/* Submit a connection to the thread pool.
 * If no idle workers available, grows the pool on demand. */
static int pool_submit(conn_t *c)
{
    pthread_mutex_lock(&g_pool.mutex);
    if (g_pool.count >= g_pool.capacity) {
        pthread_mutex_unlock(&g_pool.mutex);
        LOG_ERR("thread pool queue full");
        return -1;
    }
    g_pool.queue[g_pool.tail] = c;
    g_pool.tail = (g_pool.tail + 1) % g_pool.capacity;
    g_pool.count++;

    /* Grow pool if all workers are busy */
    if (g_pool.idle == 0 && g_pool.size < DAEMON_POOL_MAX) {
        pool_create_thread();
        LOG_DBG("pool grew to %d workers", g_pool.size);
    }

    pthread_cond_signal(&g_pool.cond);
    pthread_mutex_unlock(&g_pool.mutex);
    return 0;
}

/* Initialize the thread pool with DAEMON_POOL_SIZE initial workers.
 * Each thread has a 256KB stack (vs 8MB default). Pool grows on
 * demand up to DAEMON_POOL_MAX when all workers are busy. */
static int pool_init(void)
{
    memset(&g_pool, 0, sizeof(g_pool));
    g_pool.capacity = DAEMON_POOL_MAX * 2;
    pthread_mutex_init(&g_pool.mutex, NULL);
    pthread_cond_init(&g_pool.cond, NULL);

    for (int i = 0; i < DAEMON_POOL_SIZE; i++) {
        if (pool_create_thread() != 0) {
            LOG_ERR("pool_init: failed at thread %d", i);
            break;
        }
    }
    LOG_INFO("thread pool: %d workers (256KB stack, max %d)", g_pool.size, DAEMON_POOL_MAX);
    return g_pool.size > 0 ? 0 : -1;
}

/* Shut down the thread pool and join all threads. */
static void pool_shutdown(void)
{
    pthread_mutex_lock(&g_pool.mutex);
    g_pool.shutdown = 1;
    pthread_cond_broadcast(&g_pool.cond);
    pthread_mutex_unlock(&g_pool.mutex);

    for (int i = 0; i < g_pool.size; i++) {
        pthread_join(g_pool.threads[i], NULL);
    }
    pthread_mutex_destroy(&g_pool.mutex);
    pthread_cond_destroy(&g_pool.cond);
}

/* ================================================================
 * Accept a new connection: read DSN, setup shm, send shm name
 * ================================================================ */

static void accept_connection(int client_fd)
{
    /* Read DSN: [u32 dsn_len][dsn bytes] */
    uint8_t lenbuf[4];
    if (read_all(client_fd, lenbuf, 4) < 0) {
        LOG_ERR("failed to read DSN length");
        close(client_fd);
        return;
    }

    uint32_t dsn_len = rd_u32(lenbuf);
    if (dsn_len >= 4096) {
        LOG_ERR("DSN too long: %u", dsn_len);
        close(client_fd);
        return;
    }

    char dsn[4096];
    memset(dsn, 0, sizeof(dsn));
    if (dsn_len > 0) {
        if (read_all(client_fd, dsn, dsn_len) < 0) {
            LOG_ERR("failed to read DSN data");
            close(client_fd);
            return;
        }
    }
    dsn[dsn_len] = '\0';

    /* Error response helper: [RESP_ERROR][0] — always 2 bytes so client
     * framing (expects n >= 2) works consistently for both OK and error. */
    #define SEND_ERR_AND_CLOSE() do { \
        uint8_t _err[2] = { RESP_ERROR, 0 }; \
        (void)write_all(client_fd, _err, 2); \
        close(client_fd); \
    } while (0)

    /* Acquire a cloned DB handle */
    StoolapDB *db = db_registry_acquire(dsn);
    if (!db) {
        SEND_ERR_AND_CLOSE();
        return;
    }

    /* Create shared memory */
    char shm_name[32];
    make_shm_name(shm_name, sizeof(shm_name));
    void *shm_base = shm_create(shm_name);
    if (!shm_base) {
        stoolap_close(db);
        db_registry_release(dsn);
        SEND_ERR_AND_CLOSE();
        return;
    }

    /* Allocate connection state and spawn thread BEFORE sending handshake.
     * This ensures the client never receives OK for a connection that
     * can't actually process requests. */
    conn_t *c = calloc(1, sizeof(conn_t));
    if (!c) {
        LOG_ERR("calloc(conn_t) failed");
        munmap(shm_base, SHM_TOTAL_SIZE);
        shm_unlink(shm_name);
        stoolap_close(db);
        db_registry_release(dsn);
        SEND_ERR_AND_CLOSE();
        return;
    }

    c->client_fd   = client_fd;
    c->shm_base    = shm_base;
    strncpy(c->shm_name, shm_name, sizeof(c->shm_name) - 1);
    c->db          = db;
    strncpy(c->dsn, dsn, sizeof(c->dsn) - 1);
    c->dsn[sizeof(c->dsn) - 1] = '\0';
    c->next_stmt_id = 0;
    c->next_tx_id   = 0;

    /* Increment global connection count */
    pthread_mutex_lock(&g_conn_count_mutex);
    g_conn_count++;
    pthread_mutex_unlock(&g_conn_count_mutex);

    /* Submit to thread pool (no pthread_create per connection) */
    if (pool_submit(c) != 0) {
        LOG_ERR("pool_submit failed");
        /* conn_cleanup closes client_fd, so don't SEND_ERR_AND_CLOSE after */
        conn_cleanup(c);
        free(c);
        return;
    }

    /* Send handshake AFTER pool worker can process OP_OPEN.
     * Format: [u8 status][u8 shm_name_len][shm_name bytes] */
    {
        uint8_t name_len = (uint8_t)strlen(shm_name);
        size_t data_len = 1 + 1 + name_len;
        uint8_t data_buf[64];
        data_buf[0] = RESP_OK;
        data_buf[1] = name_len;
        memcpy(data_buf + 2, shm_name, name_len);

        if (write_all(client_fd, data_buf, data_len) < 0) {
            LOG_ERR("handshake write failed after thread spawn");
            close(client_fd); /* triggers POLLHUP — thread will clean up */
        }
    }

    #undef SEND_ERR_AND_CLOSE
    LOG_DBG("accepted connection shm=%s dsn=%s", shm_name, dsn);
}

/* ================================================================
 * Signal handlers
 * ================================================================ */

static void sigterm_handler(int sig)
{
    (void)sig;
    g_shutdown = 1;
    /* Wake accept loop */
    uint8_t wake = 1;
    (void)write(g_self_pipe[1], &wake, 1);
}

/* ================================================================
 * Create and bind the Unix domain socket
 * ================================================================ */

static int create_listen_socket(void)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        LOG_ERR("socket() failed: %s", strerror(errno));
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, STOOLAP_DAEMON_SOCK, sizeof(addr.sun_path) - 1);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        if (errno == EADDRINUSE) {
            /* Another daemon won the race — exit silently */
            close(fd);
            return -2; /* special: not an error, just lost the race */
        }
        LOG_ERR("bind(%s) failed: %s", STOOLAP_DAEMON_SOCK, strerror(errno));
        close(fd);
        return -1;
    }

    if (listen(fd, 16) < 0) {
        LOG_ERR("listen() failed: %s", strerror(errno));
        close(fd);
        unlink(STOOLAP_DAEMON_SOCK);
        return -1;
    }

    return fd;
}

/* Lock file removed — socket path is per-PID, so only one process
 * can ever fork this specific daemon. No cross-process race possible. */

/* ================================================================
 * Daemon entry point — called after fork(), never returns
 * ================================================================ */

/* Saved by MINIT before any fork */
static char *g_saved_argv0 = NULL;
static size_t g_saved_argv0_len = 0;
static pid_t g_parent_pid = 0;

void stoolap_daemon_set_parent(pid_t pid)
{
    g_parent_pid = pid;
}

void stoolap_save_argv0(char *argv0)
{
    g_saved_argv0 = argv0;
    if (argv0) {
        /* Measure full argv space: walk past all argv strings (null-separated) */
#if defined(__APPLE__)
        extern int *_NSGetArgc(void);
        extern char ***_NSGetArgv(void);
        int argc = *_NSGetArgc();
        char **argv = *_NSGetArgv();
        if (argc > 0 && argv) {
            char *last = argv[argc - 1];
            char *end = last;
            while (*end) end++;
            g_saved_argv0_len = end - argv0 + 1;
        } else {
            char *end = argv0;
            while (*end) end++;
            g_saved_argv0_len = end - argv0;
        }
#else
        char *end = argv0;
        while (*end) end++;
        g_saved_argv0_len = end - argv0;
#endif
    }
}

void stoolap_daemon_run(char *argv0_unused, size_t argv0_unused_len)
{
    (void)argv0_unused;
    (void)argv0_unused_len;

    /* Detach from terminal */
    setsid();

    /* Build process title: "stoolap: <binary_name>"
     * Extract binary name from the original argv[0] before overwriting */
    char title[128];
    if (g_saved_argv0 && g_saved_argv0_len > 0) {
        /* Find binary name — take chars before first ':' or space */
        const char *bin = g_saved_argv0;
        const char *slash = strrchr(bin, '/');
        if (slash) bin = slash + 1;
        size_t bin_len = 0;
        while (bin[bin_len] && bin[bin_len] != ':' && bin[bin_len] != ' ' && bin_len < 60)
            bin_len++;
        snprintf(title, sizeof(title), "stoolap: %.*s", (int)bin_len, bin);

        size_t tlen = strlen(title);
        memset(g_saved_argv0, 0, g_saved_argv0_len);
        memcpy(g_saved_argv0, title, tlen < g_saved_argv0_len ? tlen : g_saved_argv0_len - 1);
    } else {
        snprintf(title, sizeof(title), "stoolap: php");
    }
#if defined(__APPLE__)
    setprogname(title);
    pthread_setname_np(title);
#elif defined(__linux__)
    prctl(PR_SET_NAME, title, 0, 0, 0);
#endif

    /* Close inherited stdio (we keep stderr for logging) */
    close(STDIN_FILENO);
    open("/dev/null", O_RDONLY);
    close(STDOUT_FILENO);
    open("/dev/null", O_WRONLY);

    signal(SIGPIPE, SIG_IGN);

    /* Install SIGTERM handler */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigterm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);

    /* Self-pipe for waking accept loop on signal/shutdown */
    if (pipe(g_self_pipe) < 0) {
        LOG_ERR("pipe() for self-pipe failed: %s", strerror(errno));
        _exit(1);
    }
    set_nonblocking(g_self_pipe[0]);
    set_nonblocking(g_self_pipe[1]);

    /* Create listen socket — bind() is the atomic lock.
     * If another daemon already bound, we get -2 and exit silently. */
    int listen_fd = create_listen_socket();
    if (listen_fd == -2) {
        _exit(0); /* another daemon won the race */
    }
    if (listen_fd < 0) {
        _exit(1);
    }

    /* Initialize thread pool */
    if (pool_init() != 0) {
        LOG_ERR("thread pool init failed");
        close(listen_fd);
        unlink(STOOLAP_DAEMON_SOCK);
        _exit(1);
    }

    LOG_INFO("started, pid=%d, socket=%s, pool=%d", (int)getpid(), STOOLAP_DAEMON_SOCK, g_pool.size);

    /* ================================================================
     * Accept loop
     * ================================================================ */

    struct pollfd pfds[2];
    pfds[0].fd     = listen_fd;
    pfds[0].events = POLLIN;
    pfds[1].fd     = g_self_pipe[0];
    pfds[1].events = POLLIN;

    while (!g_shutdown) {
        int timeout_ms;
        pthread_mutex_lock(&g_conn_count_mutex);
        int conns = g_conn_count;
        pthread_mutex_unlock(&g_conn_count_mutex);

        if (conns == 0) {
            /* Check if parent (fpm master) is still alive */
            if (g_parent_pid > 0 && kill(g_parent_pid, 0) != 0) {
                LOG_INFO("parent process %d gone, shutting down", (int)g_parent_pid);
                break;
            }
            timeout_ms = 5000;
        } else {
            timeout_ms = -1; /* block when active */
        }

        int nready = poll(pfds, 2, timeout_ms);
        if (nready < 0) {
            if (errno == EINTR) continue;
            LOG_ERR("accept poll failed: %s", strerror(errno));
            break;
        }

        if (nready == 0) {
            /* Timeout — check idle again */
            continue;
        }

        /* Self-pipe: signal or shutdown notification */
        if (pfds[1].revents & POLLIN) {
            uint8_t buf[16];
            while (read(g_self_pipe[0], buf, sizeof(buf)) > 0)
                ; /* drain */
            if (g_shutdown) break;
        }

        /* New connection on listen socket */
        if (pfds[0].revents & POLLIN) {
            int client_fd = accept(listen_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR || errno == EAGAIN) continue;
                LOG_ERR("accept() failed: %s", strerror(errno));
                continue;
            }
            accept_connection(client_fd);
        }
    }

    /* ================================================================
     * Shutdown
     * ================================================================ */

    LOG_INFO("shutting down...");

    /* Stop accepting new connections */
    close(listen_fd);
    unlink(STOOLAP_DAEMON_SOCK);

    /* Shut down thread pool — signals all workers and joins them.
     * Active connections will finish their current request, then
     * workers detect shutdown and exit cleanly. */
    pool_shutdown();

    /* Close all master database handles */
    db_registry_close_all();

    /* Cleanup */
    close(g_self_pipe[0]);
    close(g_self_pipe[1]);

    LOG_INFO("exiting");
    _exit(0);
}
