#ifndef PHP_STOOLAP_H
#define PHP_STOOLAP_H

extern zend_module_entry stoolap_module_entry;
#define phpext_stoolap_ptr &stoolap_module_entry

#define PHP_STOOLAP_VERSION "0.3.5"

/* Stoolap C API types */
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

/* Stoolap C API functions */
extern const char *stoolap_version(void);

extern int32_t stoolap_open(const char *dsn, StoolapDB **out_db);
extern int32_t stoolap_open_in_memory(StoolapDB **out_db);
extern int32_t stoolap_clone(const StoolapDB *db, StoolapDB **out_db);
extern int32_t stoolap_close(StoolapDB *db);
extern const char *stoolap_errmsg(const StoolapDB *db);

extern int32_t stoolap_exec(StoolapDB *db, const char *sql, int64_t *rows_affected);
extern int32_t stoolap_exec_params(StoolapDB *db, const char *sql, const StoolapValue *params, int32_t params_len, int64_t *rows_affected);
extern int32_t stoolap_query(StoolapDB *db, const char *sql, StoolapRows **out_rows);
extern int32_t stoolap_query_params(StoolapDB *db, const char *sql, const StoolapValue *params, int32_t params_len, StoolapRows **out_rows);

extern int32_t stoolap_prepare(StoolapDB *db, const char *sql, StoolapStmt **out_stmt);
extern int32_t stoolap_stmt_exec(StoolapStmt *stmt, const StoolapValue *params, int32_t params_len, int64_t *rows_affected);
extern int32_t stoolap_stmt_query(StoolapStmt *stmt, const StoolapValue *params, int32_t params_len, StoolapRows **out_rows);
extern void stoolap_stmt_finalize(StoolapStmt *stmt);
extern const char *stoolap_stmt_errmsg(const StoolapStmt *stmt);

extern int32_t stoolap_begin(StoolapDB *db, StoolapTx **out_tx);
extern int32_t stoolap_begin_with_isolation(StoolapDB *db, int32_t isolation, StoolapTx **out_tx);
extern int32_t stoolap_tx_exec(StoolapTx *tx, const char *sql, int64_t *rows_affected);
extern int32_t stoolap_tx_exec_params(StoolapTx *tx, const char *sql, const StoolapValue *params, int32_t params_len, int64_t *rows_affected);
extern int32_t stoolap_tx_query(StoolapTx *tx, const char *sql, StoolapRows **out_rows);
extern int32_t stoolap_tx_query_params(StoolapTx *tx, const char *sql, const StoolapValue *params, int32_t params_len, StoolapRows **out_rows);
extern int32_t stoolap_tx_stmt_exec(StoolapTx *tx, const StoolapStmt *stmt, const StoolapValue *params, int32_t params_len, int64_t *rows_affected);
extern int32_t stoolap_tx_stmt_query(StoolapTx *tx, const StoolapStmt *stmt, const StoolapValue *params, int32_t params_len, StoolapRows **out_rows);
extern int32_t stoolap_tx_commit(StoolapTx *tx);
extern int32_t stoolap_tx_rollback(StoolapTx *tx);
extern const char *stoolap_tx_errmsg(const StoolapTx *tx);

extern int32_t stoolap_rows_fetch_all(StoolapRows *rows, uint8_t **out_buf, int64_t *out_len);
extern void stoolap_rows_close(StoolapRows *rows);
extern const char *stoolap_rows_errmsg(const StoolapRows *rows);
extern void stoolap_buffer_free(uint8_t *buf, int64_t len);

#endif
