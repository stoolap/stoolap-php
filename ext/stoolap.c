#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "ext/json/php_json.h"
#include "zend_exceptions.h"
#include "zend_interfaces.h"
#include "zend_smart_str.h"
#include <ctype.h>
#include <inttypes.h>
#include <time.h>

#include "ext/spl/spl_exceptions.h"
#include "php_stoolap.h"

/* ================================================================
 * Class entries and object handlers
 * ================================================================ */

static zend_class_entry *stoolap_database_ce;
static zend_class_entry *stoolap_statement_ce;
static zend_class_entry *stoolap_transaction_ce;
static zend_class_entry *stoolap_exception_ce;

static zend_object_handlers stoolap_database_handlers;
static zend_object_handlers stoolap_statement_handlers;
static zend_object_handlers stoolap_transaction_handlers;

static zend_class_entry *datetime_interface_ce;

/* ================================================================
 * Object structs
 * ================================================================ */

typedef struct {
    StoolapDB *db;
    zend_object std;
} stoolap_db_obj;

typedef struct {
    StoolapStmt *stmt;
    zend_string *sql;
    zend_string **param_names;
    uint32_t param_name_count;
    StoolapValue *cached_values;
    int32_t cached_count;
    zval db_zv;     /* strong reference to Database object — prevents GC */
    zend_object std;
} stoolap_stmt_obj;

typedef struct {
    StoolapTx *tx;
    StoolapDB *db;
    zval db_zv;     /* strong reference to Database object — prevents GC */
    zend_object std;
} stoolap_tx_obj;

/* Object-from-zend_object macros */
static inline stoolap_db_obj *stoolap_db_from_obj(zend_object *obj) {
    return (stoolap_db_obj *)((char *)obj - XtOffsetOf(stoolap_db_obj, std));
}
static inline stoolap_stmt_obj *stoolap_stmt_from_obj(zend_object *obj) {
    return (stoolap_stmt_obj *)((char *)obj - XtOffsetOf(stoolap_stmt_obj, std));
}
static inline stoolap_tx_obj *stoolap_tx_from_obj(zend_object *obj) {
    return (stoolap_tx_obj *)((char *)obj - XtOffsetOf(stoolap_tx_obj, std));
}

#define Z_STOOLAP_DB_P(zv) stoolap_db_from_obj(Z_OBJ_P(zv))
#define Z_STOOLAP_STMT_P(zv) stoolap_stmt_from_obj(Z_OBJ_P(zv))
#define Z_STOOLAP_TX_P(zv) stoolap_tx_from_obj(Z_OBJ_P(zv))

/* ================================================================
 * Buffer reading helpers (little-endian)
 * ================================================================ */

static inline uint16_t buf_u16(const uint8_t *p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline uint32_t buf_u32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline int64_t  buf_i64(const uint8_t *p) { int64_t  v; memcpy(&v, p, 8); return v; }
static inline double   buf_f64(const uint8_t *p) { double   v; memcpy(&v, p, 8); return v; }

/* Column name reference into buffer (avoids zend_string alloc per query) */
typedef struct {
    const char *ptr;
    uint16_t len;
} col_ref_t;

/* Decode a JSON value from the buffer. Uses stack buffer for small JSON,
 * heap for large. php_json_decode_ex requires null-terminated input. */
static inline void decode_json_value(const uint8_t *data, uint32_t len, zval *decoded)
{
    char stack_buf[256];
    char *json_str;
    if (len < sizeof(stack_buf)) {
        json_str = stack_buf;
    } else {
        json_str = emalloc(len + 1);
    }
    memcpy(json_str, data, len);
    json_str[len] = '\0';
    php_json_decode_ex(decoded, json_str, len, PHP_JSON_OBJECT_AS_ARRAY, 512);
    if (json_str != stack_buf) efree(json_str);
}

/* Format timestamp nanos → ISO 8601 string, returns length written */
static inline int format_timestamp(int64_t nanos, char *tsbuf, size_t tsbuf_size)
{
    int64_t secs = nanos / 1000000000LL;
    int64_t frac = nanos % 1000000000LL;
    if (frac < 0) { secs--; frac += 1000000000LL; }
    time_t t = (time_t)secs;
    struct tm tm_buf;
    struct tm *tm = gmtime_r(&t, &tm_buf);
    return snprintf(tsbuf, tsbuf_size,
        "%04d-%02d-%02dT%02d:%02d:%02d.%09" PRId64 "Z",
        tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
        tm->tm_hour, tm->tm_min, tm->tm_sec, frac);
}

/* Convert DateTimeInterface → nanoseconds.
 * Returns 0 on success, -1 on failure (exception thrown or propagated). */
static int datetime_to_nanos(zval *dt_obj, int64_t *out_nanos)
{
    zval func_name, retval;

    ZVAL_STRING(&func_name, "getTimestamp");
    int rc = call_user_function(NULL, dt_obj, &func_name, &retval, 0, NULL);
    zval_ptr_dtor(&func_name);
    if (rc == FAILURE || EG(exception) || Z_TYPE(retval) != IS_LONG) {
        zval_ptr_dtor(&retval);
        if (!EG(exception)) {
            zend_throw_exception(stoolap_exception_ce,
                "Failed to get timestamp from DateTimeInterface object", 0);
        }
        return -1;
    }
    int64_t secs = Z_LVAL(retval);
    zval_ptr_dtor(&retval);

    zval arg;
    ZVAL_STRING(&func_name, "format");
    ZVAL_STRING(&arg, "u");
    rc = call_user_function(NULL, dt_obj, &func_name, &retval, 1, &arg);
    zval_ptr_dtor(&func_name);
    zval_ptr_dtor(&arg);
    if (rc == FAILURE || EG(exception) || Z_TYPE(retval) != IS_STRING) {
        zval_ptr_dtor(&retval);
        if (!EG(exception)) {
            zend_throw_exception(stoolap_exception_ce,
                "Failed to format DateTimeInterface microseconds", 0);
        }
        return -1;
    }
    int64_t micro = strtoll(Z_STRVAL(retval), NULL, 10);
    zval_ptr_dtor(&retval);

    *out_nanos = secs * 1000000000LL + micro * 1000LL;
    return 0;
}

/* Parse one row from buffer into an assoc array using col_ref_t names */
static inline void parse_one_row_assoc(const uint8_t *buf, size_t *off,
    const col_ref_t *cols, uint32_t col_count, zval *row)
{
    array_init_size(row, col_count);
    for (uint32_t c = 0; c < col_count; c++) {
        uint8_t type = buf[(*off)++];
        switch (type) {
            case 0:
                add_assoc_null_ex(row, cols[c].ptr, cols[c].len);
                break;
            case 1: {
                int64_t v = buf_i64(buf + *off); *off += 8;
                add_assoc_long_ex(row, cols[c].ptr, cols[c].len, (zend_long)v);
                break;
            }
            case 2: {
                double v = buf_f64(buf + *off); *off += 8;
                add_assoc_double_ex(row, cols[c].ptr, cols[c].len, v);
                break;
            }
            case 3: {
                uint32_t len = buf_u32(buf + *off); *off += 4;
                add_assoc_stringl_ex(row, cols[c].ptr, cols[c].len,
                                     (const char *)(buf + *off), len);
                *off += len;
                break;
            }
            case 4: {
                uint8_t v = buf[(*off)++];
                add_assoc_bool_ex(row, cols[c].ptr, cols[c].len, v != 0);
                break;
            }
            case 5: {
                int64_t nanos = buf_i64(buf + *off); *off += 8;
                char tsbuf[64];
                int tslen = format_timestamp(nanos, tsbuf, sizeof(tsbuf));
                add_assoc_stringl_ex(row, cols[c].ptr, cols[c].len, tsbuf, tslen);
                break;
            }
            case 6: {
                uint32_t len = buf_u32(buf + *off); *off += 4;
                zval decoded;
                decode_json_value(buf + *off, len, &decoded);
                *off += len;
                add_assoc_zval_ex(row, cols[c].ptr, cols[c].len, &decoded);
                break;
            }
            case 7: {
                uint32_t len = buf_u32(buf + *off); *off += 4;
                add_assoc_stringl_ex(row, cols[c].ptr, cols[c].len,
                                     (const char *)(buf + *off), len);
                *off += len;
                break;
            }
        }
    }
}

/* ================================================================
 * Parse binary buffer → PHP associative array
 *
 * Uses zend_string* for column names so the hash is computed once
 * and reused for every row (avoids per-row rehashing).
 * Stack-allocates the column array for ≤32 columns.
 * ================================================================ */

static void parse_buffer_assoc(const uint8_t *buf, size_t buf_len, zval *return_value)
{
    size_t off = 0;
    uint32_t col_count = buf_u32(buf + off); off += 4;

    /* Build zend_string* column names — hash computed once, reused per row */
    zend_string *stack_strs[32];
    zend_string **col_strs = (col_count <= 32) ? stack_strs
                                               : emalloc(sizeof(zend_string *) * col_count);
    for (uint32_t i = 0; i < col_count; i++) {
        uint16_t name_len = buf_u16(buf + off); off += 2;
        col_strs[i] = zend_string_init((const char *)(buf + off), name_len, 0);
        off += name_len;
    }

    uint32_t row_count = buf_u32(buf + off); off += 4;
    array_init_size(return_value, row_count);

    for (uint32_t r = 0; r < row_count; r++) {
        zval row;
        array_init_size(&row, col_count);

        for (uint32_t c = 0; c < col_count; c++) {
            uint8_t type = buf[off++];
            zval tmp;
            switch (type) {
                case 0:
                    ZVAL_NULL(&tmp);
                    break;
                case 1: {
                    int64_t v = buf_i64(buf + off); off += 8;
                    ZVAL_LONG(&tmp, (zend_long)v);
                    break;
                }
                case 2: {
                    double v = buf_f64(buf + off); off += 8;
                    ZVAL_DOUBLE(&tmp, v);
                    break;
                }
                case 3: {
                    uint32_t len = buf_u32(buf + off); off += 4;
                    ZVAL_STRINGL(&tmp, (const char *)(buf + off), len);
                    off += len;
                    break;
                }
                case 4: {
                    uint8_t v = buf[off++];
                    ZVAL_BOOL(&tmp, v != 0);
                    break;
                }
                case 5: {
                    int64_t nanos = buf_i64(buf + off); off += 8;
                    char tsbuf[64];
                    int tslen = format_timestamp(nanos, tsbuf, sizeof(tsbuf));
                    ZVAL_STRINGL(&tmp, tsbuf, tslen);
                    break;
                }
                case 6: {
                    uint32_t len = buf_u32(buf + off); off += 4;
                    decode_json_value(buf + off, len, &tmp);
                    off += len;
                    break;
                }
                case 7: {
                    uint32_t len = buf_u32(buf + off); off += 4;
                    ZVAL_STRINGL(&tmp, (const char *)(buf + off), len);
                    off += len;
                    break;
                }
            }
            zend_symtable_update(Z_ARRVAL(row), col_strs[c], &tmp);
        }

        add_next_index_zval(return_value, &row);
    }

    for (uint32_t i = 0; i < col_count; i++) {
        zend_string_release(col_strs[i]);
    }
    if (col_strs != stack_strs) efree(col_strs);
}

/* ================================================================
 * Parse binary buffer → first row only (for queryOne)
 * ================================================================ */

static void parse_buffer_one(const uint8_t *buf, size_t buf_len, zval *return_value)
{
    size_t off = 0;
    uint32_t col_count = buf_u32(buf + off); off += 4;

    /* Stack-allocate column refs for typical queries (≤32 columns) */
    col_ref_t stack_cols[32];
    col_ref_t *cols = (col_count <= 32) ? stack_cols
                                        : emalloc(sizeof(col_ref_t) * col_count);
    for (uint32_t i = 0; i < col_count; i++) {
        cols[i].len = buf_u16(buf + off); off += 2;
        cols[i].ptr = (const char *)(buf + off);
        off += cols[i].len;
    }

    uint32_t row_count = buf_u32(buf + off); off += 4;

    if (row_count == 0) {
        ZVAL_NULL(return_value);
    } else {
        parse_one_row_assoc(buf, &off, cols, col_count, return_value);
    }

    if (cols != stack_cols) efree(cols);
}

/* ================================================================
 * Parse binary buffer → raw format
 * ================================================================ */

static void parse_buffer_raw(const uint8_t *buf, size_t buf_len, zval *return_value)
{
    size_t off = 0;

    uint32_t col_count = buf_u32(buf + off); off += 4;

    zval columns;
    array_init_size(&columns, col_count);
    for (uint32_t i = 0; i < col_count; i++) {
        uint16_t name_len = buf_u16(buf + off); off += 2;
        add_next_index_stringl(&columns, (const char *)(buf + off), name_len);
        off += name_len;
    }

    uint32_t row_count = buf_u32(buf + off); off += 4;

    zval rows;
    array_init_size(&rows, row_count);

    for (uint32_t r = 0; r < row_count; r++) {
        zval row;
        array_init_size(&row, col_count);

        for (uint32_t c = 0; c < col_count; c++) {
            uint8_t type = buf[off++];
            switch (type) {
                case 0: add_next_index_null(&row); break;
                case 1: { int64_t v = buf_i64(buf + off); off += 8; add_next_index_long(&row, (zend_long)v); break; }
                case 2: { double v = buf_f64(buf + off); off += 8; add_next_index_double(&row, v); break; }
                case 3: {
                    uint32_t len = buf_u32(buf + off); off += 4;
                    add_next_index_stringl(&row, (const char *)(buf + off), len);
                    off += len;
                    break;
                }
                case 4: { uint8_t v = buf[off++]; add_next_index_bool(&row, v != 0); break; }
                case 5: {
                    int64_t nanos = buf_i64(buf + off); off += 8;
                    char tsbuf[64];
                    int tslen = format_timestamp(nanos, tsbuf, sizeof(tsbuf));
                    add_next_index_stringl(&row, tsbuf, tslen);
                    break;
                }
                case 6: {
                    uint32_t len = buf_u32(buf + off); off += 4;
                    zval decoded;
                    decode_json_value(buf + off, len, &decoded);
                    off += len;
                    add_next_index_zval(&row, &decoded);
                    break;
                }
                case 7: {
                    uint32_t len = buf_u32(buf + off); off += 4;
                    add_next_index_stringl(&row, (const char *)(buf + off), len);
                    off += len;
                    break;
                }
            }
        }
        add_next_index_zval(&rows, &row);
    }

    array_init_size(return_value, 2);
    add_assoc_zval(return_value, "columns", &columns);
    add_assoc_zval(return_value, "rows", &rows);
}

/* ================================================================
 * Handle rows: fetch_all + parse + cleanup
 * ================================================================ */

enum { QUERY_ASSOC = 0, QUERY_ONE = 1, QUERY_RAW = 2 };

static int stoolap_handle_rows(StoolapRows *rows, zval *return_value, int mode)
{
    uint8_t *buf = NULL;
    int64_t buf_len = 0;

    int32_t rc = stoolap_rows_fetch_all(rows, &buf, &buf_len);
    if (rc != 0) {
        const char *err = stoolap_rows_errmsg(rows);
        /* Throw before close — close frees the error string */
        zend_throw_exception(stoolap_exception_ce, err ? err : "Failed to fetch rows", 0);
        stoolap_rows_close(rows);
        return FAILURE;
    }

    if (mode == QUERY_RAW) {
        parse_buffer_raw(buf, (size_t)buf_len, return_value);
    } else if (mode == QUERY_ONE) {
        parse_buffer_one(buf, (size_t)buf_len, return_value);
    } else {
        parse_buffer_assoc(buf, (size_t)buf_len, return_value);
    }

    stoolap_buffer_free(buf, buf_len);
    stoolap_rows_close(rows);
    return SUCCESS;
}

/* ================================================================
 * Check if SQL already has a LIMIT clause (outside quotes/comments)
 * ================================================================ */

/* SQL identifier char: alphanumeric or underscore */
static inline int is_ident(unsigned char c) { return isalnum(c) || c == '_'; }

static int sql_has_limit(const char *sql, size_t len)
{
    const char *p = sql;
    const char *end = sql + len;
    int depth = 0; /* parenthesis depth — only match LIMIT at outer level */

    while (p < end) {
        if (*p == '\'' || *p == '"') {
            char q = *p++;
            while (p < end) {
                if (*p == q) { p++; if (p < end && *p == q) p++; else break; }
                else p++;
            }
        } else if (*p == '/' && p + 1 < end && p[1] == '*') {
            p += 2;
            while (p + 1 < end && !(*p == '*' && p[1] == '/')) p++;
            if (p + 1 < end) p += 2;
        } else if (*p == '-' && p + 1 < end && p[1] == '-') {
            p += 2;
            while (p < end && *p != '\n') p++;
        } else if (*p == '(') {
            depth++; p++;
        } else if (*p == ')') {
            if (depth > 0) depth--;
            p++;
        } else if (depth == 0 && (end - p) >= 5 &&
                   (p[0] == 'L' || p[0] == 'l') &&
                   (p[1] == 'I' || p[1] == 'i') &&
                   (p[2] == 'M' || p[2] == 'm') &&
                   (p[3] == 'I' || p[3] == 'i') &&
                   (p[4] == 'T' || p[4] == 't') &&
                   (p == sql || !is_ident((unsigned char)p[-1])) &&
                   (p + 5 >= end || !is_ident((unsigned char)p[5]))) {
            return 1;
        } else {
            p++;
        }
    }
    return 0;
}

/* Find the end of the SQL body before any trailing comments / whitespace.
 * Scans forward (properly skipping strings, block/line comments) and tracks
 * the byte offset just past the last meaningful SQL character. */
static size_t sql_body_end(const char *sql, size_t len)
{
    const char *p = sql;
    const char *end = sql + len;
    size_t body_end = 0;

    while (p < end) {
        if (*p == '\'' || *p == '"') {
            char q = *p++;
            while (p < end) {
                if (*p == q) { p++; if (p < end && *p == q) p++; else break; }
                else p++;
            }
            body_end = (size_t)(p - sql);
        } else if (*p == '/' && p + 1 < end && p[1] == '*') {
            /* block comment — skip without advancing body_end */
            p += 2;
            while (p + 1 < end && !(*p == '*' && p[1] == '/')) p++;
            if (p + 1 < end) p += 2;
        } else if (*p == '-' && p + 1 < end && p[1] == '-') {
            /* line comment — skip without advancing body_end */
            p += 2;
            while (p < end && *p != '\n') p++;
        } else if (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') {
            /* whitespace — skip without advancing body_end */
            p++;
        } else {
            p++;
            body_end = (size_t)(p - sql);
        }
    }
    return body_end;
}

/* Insert " LIMIT 1" at the end of the SQL body (before trailing comments
 * and whitespace) if no LIMIT clause is present.
 * Returns emalloc'd copy or NULL (no change needed). */
static char *ensure_limit_one(const char *sql, size_t sql_len, size_t *out_len)
{
    if (sql_has_limit(sql, sql_len)) {
        *out_len = sql_len;
        return NULL;
    }
    size_t body = sql_body_end(sql, sql_len);
    size_t tail_len = sql_len - body;
    size_t new_len = body + 8 + tail_len; /* body + " LIMIT 1" + trailing */
    char *result = emalloc(new_len + 1);
    memcpy(result, sql, body);
    memcpy(result + body, " LIMIT 1", 8);
    if (tail_len > 0) {
        memcpy(result + body + 8, sql + body, tail_len);
    }
    result[new_len] = '\0';
    *out_len = new_len;
    return result;
}

/* ================================================================
 * Strip trailing semicolon (returns emalloc'd copy or NULL)
 * ================================================================ */

static char *strip_semicolon(const char *sql, size_t sql_len, size_t *out_len)
{
    size_t len = sql_len;
    while (len > 0 && (sql[len - 1] == ' ' || sql[len - 1] == '\t' ||
                       sql[len - 1] == '\n' || sql[len - 1] == '\r')) {
        len--;
    }
    if (len > 0 && sql[len - 1] == ';') {
        len--;
        char *result = emalloc(len + 1);
        memcpy(result, sql, len);
        result[len] = '\0';
        *out_len = len;
        return result;
    }
    *out_len = sql_len;
    return NULL;
}

/* ================================================================
 * Build StoolapValue array from PHP array
 * ================================================================ */

typedef struct {
    StoolapValue *values;
    int32_t count;
    zend_string **json_strs;
    int json_count;
    int json_alloc;
} stoolap_params_t;

static void stoolap_params_init(stoolap_params_t *p) {
    memset(p, 0, sizeof(*p));
}

static void stoolap_params_free(stoolap_params_t *p) {
    for (int i = 0; i < p->json_count; i++) {
        zend_string_release(p->json_strs[i]);
    }
    if (p->json_strs) efree(p->json_strs);
    if (p->values) efree(p->values);
    memset(p, 0, sizeof(*p));
}

static int stoolap_build_params(zval *params, stoolap_params_t *out)
{
    stoolap_params_init(out);

    if (params == NULL || Z_TYPE_P(params) != IS_ARRAY) {
        return SUCCESS;
    }

    HashTable *ht = Z_ARRVAL_P(params);
    int32_t count = zend_hash_num_elements(ht);
    if (count == 0) return SUCCESS;

    StoolapValue *values = emalloc(count * sizeof(StoolapValue));
    out->values = values;
    out->count = count;

    int idx = 0;
    zval *val;
    ZEND_HASH_FOREACH_VAL(ht, val) {
        values[idx]._padding = 0;

        if (Z_TYPE_P(val) == IS_NULL) {
            values[idx].value_type = 0;
        } else if (Z_TYPE_P(val) == IS_TRUE) {
            values[idx].value_type = 4;
            values[idx].v.boolean = 1;
        } else if (Z_TYPE_P(val) == IS_FALSE) {
            values[idx].value_type = 4;
            values[idx].v.boolean = 0;
        } else if (Z_TYPE_P(val) == IS_LONG) {
            values[idx].value_type = 1;
            values[idx].v.integer = Z_LVAL_P(val);
        } else if (Z_TYPE_P(val) == IS_DOUBLE) {
            values[idx].value_type = 2;
            values[idx].v.float64 = Z_DVAL_P(val);
        } else if (Z_TYPE_P(val) == IS_STRING) {
            values[idx].value_type = 3;
            values[idx].v.text.ptr = Z_STRVAL_P(val);
            values[idx].v.text.len = Z_STRLEN_P(val);
        } else if (Z_TYPE_P(val) == IS_ARRAY) {
            /* JSON encode */
            smart_str buf = {0};
            if (php_json_encode(&buf, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES) == FAILURE) {
                smart_str_free(&buf);
                zend_throw_exception(stoolap_exception_ce,
                    "Failed to JSON-encode array parameter", 0);
                return FAILURE;
            }
            smart_str_0(&buf);

            values[idx].value_type = 6;
            values[idx].v.text.ptr = ZSTR_VAL(buf.s);
            values[idx].v.text.len = ZSTR_LEN(buf.s);

            /* Track for cleanup */
            if (out->json_count >= out->json_alloc) {
                out->json_alloc = out->json_alloc ? out->json_alloc * 2 : 4;
                out->json_strs = erealloc(out->json_strs, sizeof(zend_string *) * out->json_alloc);
            }
            out->json_strs[out->json_count++] = buf.s;
        } else if (Z_TYPE_P(val) == IS_OBJECT) {
            if (datetime_interface_ce && instanceof_function(Z_OBJCE_P(val), datetime_interface_ce)) {
                int64_t nanos;
                if (datetime_to_nanos(val, &nanos) != 0) {
                    return FAILURE;
                }
                values[idx].value_type = 5;
                values[idx].v.timestamp_nanos = nanos;
            } else {
                /* JSON encode object */
                smart_str buf = {0};
                if (php_json_encode(&buf, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES) == FAILURE) {
                    smart_str_free(&buf);
                    zend_throw_exception(stoolap_exception_ce,
                        "Failed to JSON-encode object parameter", 0);
                    return FAILURE;
                }
                smart_str_0(&buf);

                values[idx].value_type = 6;
                values[idx].v.text.ptr = ZSTR_VAL(buf.s);
                values[idx].v.text.len = ZSTR_LEN(buf.s);

                if (out->json_count >= out->json_alloc) {
                    out->json_alloc = out->json_alloc ? out->json_alloc * 2 : 4;
                    out->json_strs = erealloc(out->json_strs, sizeof(zend_string *) * out->json_alloc);
                }
                out->json_strs[out->json_count++] = buf.s;
            }
        } else {
            zend_throw_exception_ex(stoolap_exception_ce, 0,
                "Unsupported parameter type: %s", zend_zval_type_name(val));
            return FAILURE;
        }

        idx++;
    } ZEND_HASH_FOREACH_END();

    return SUCCESS;
}

/* ================================================================
 * Batch-optimized param fill — reuses pre-allocated StoolapValue array
 * ================================================================ */

static inline void stoolap_batch_json_free(zend_string **json_strs, int *json_count) {
    for (int i = 0; i < *json_count; i++) {
        zend_string_release(json_strs[i]);
    }
    *json_count = 0;
}

static int stoolap_batch_fill_params(zval *params, StoolapValue *values, int32_t capacity,
                                     zend_string **json_strs, int *json_count, int json_alloc)
{
    HashTable *ht = Z_ARRVAL_P(params);
    int32_t count = zend_hash_num_elements(ht);
    if (count != capacity) return FAILURE;

    int idx = 0;
    zval *val;
    ZEND_HASH_FOREACH_VAL(ht, val) {
        values[idx]._padding = 0;

        if (Z_TYPE_P(val) == IS_NULL) {
            values[idx].value_type = 0;
        } else if (Z_TYPE_P(val) == IS_TRUE) {
            values[idx].value_type = 4;
            values[idx].v.boolean = 1;
        } else if (Z_TYPE_P(val) == IS_FALSE) {
            values[idx].value_type = 4;
            values[idx].v.boolean = 0;
        } else if (Z_TYPE_P(val) == IS_LONG) {
            values[idx].value_type = 1;
            values[idx].v.integer = Z_LVAL_P(val);
        } else if (Z_TYPE_P(val) == IS_DOUBLE) {
            values[idx].value_type = 2;
            values[idx].v.float64 = Z_DVAL_P(val);
        } else if (Z_TYPE_P(val) == IS_STRING) {
            values[idx].value_type = 3;
            values[idx].v.text.ptr = Z_STRVAL_P(val);
            values[idx].v.text.len = Z_STRLEN_P(val);
        } else if (Z_TYPE_P(val) == IS_ARRAY) {
            smart_str buf = {0};
            if (php_json_encode(&buf, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES) == FAILURE) {
                smart_str_free(&buf);
                zend_throw_exception(stoolap_exception_ce,
                    "Failed to JSON-encode array parameter", 0);
                return FAILURE;
            }
            smart_str_0(&buf);
            values[idx].value_type = 6;
            values[idx].v.text.ptr = ZSTR_VAL(buf.s);
            values[idx].v.text.len = ZSTR_LEN(buf.s);
            json_strs[(*json_count)++] = buf.s;
        } else if (Z_TYPE_P(val) == IS_OBJECT) {
            if (datetime_interface_ce && instanceof_function(Z_OBJCE_P(val), datetime_interface_ce)) {
                int64_t nanos;
                if (datetime_to_nanos(val, &nanos) != 0) {
                    return FAILURE;
                }
                values[idx].value_type = 5;
                values[idx].v.timestamp_nanos = nanos;
            } else {
                smart_str buf = {0};
                if (php_json_encode(&buf, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES) == FAILURE) {
                    smart_str_free(&buf);
                    zend_throw_exception(stoolap_exception_ce,
                        "Failed to JSON-encode object parameter", 0);
                    return FAILURE;
                }
                smart_str_0(&buf);
                values[idx].value_type = 6;
                values[idx].v.text.ptr = ZSTR_VAL(buf.s);
                values[idx].v.text.len = ZSTR_LEN(buf.s);
                json_strs[(*json_count)++] = buf.s;
            }
        } else {
            zend_throw_exception_ex(stoolap_exception_ce, 0,
                "Unsupported parameter type: %s", zend_zval_type_name(val));
            return FAILURE;
        }
        idx++;
    } ZEND_HASH_FOREACH_END();

    return SUCCESS;
}

/* ================================================================
 * Rewrite named params (:name → $N)
 * ================================================================ */

static char *rewrite_named_params_sql(const char *sql, size_t sql_len,
    zend_string ***out_names, uint32_t *out_name_count)
{
    const char *p = sql;
    const char *end = sql + sql_len;
    const char *first_colon = NULL;

    /* Quick scan for ':name' outside of quoted strings and comments */
    while (p < end) {
        if (*p == '\'' || *p == '"') {
            char quote = *p++;
            while (p < end) {
                if (*p == quote) {
                    p++;
                    if (p < end && *p == quote) { p++; } /* escaped quote */
                    else break;
                } else { p++; }
            }
        } else if (*p == '/' && (p + 1) < end && p[1] == '*') {
            /* Block comment — skip */
            p += 2;
            while (p + 1 < end && !(*p == '*' && p[1] == '/')) p++;
            if (p + 1 < end) p += 2;
        } else if (*p == '-' && (p + 1) < end && p[1] == '-') {
            /* Line comment — skip to end of line */
            p += 2;
            while (p < end && *p != '\n') p++;
        } else if (*p == ':' && (p + 1) < end && (isalpha((unsigned char)p[1]) || p[1] == '_')) {
            first_colon = p;
            break;
        } else {
            p++;
        }
    }

    if (!first_colon) {
        *out_names = NULL;
        *out_name_count = 0;
        return NULL;
    }

    zend_string **names = NULL;
    uint32_t name_count = 0;
    uint32_t name_alloc = 0;

    smart_str result = {0};

    /* Bulk-copy everything before the first ':' */
    if (first_colon > sql) {
        smart_str_appendl(&result, sql, first_colon - sql);
    }

    p = first_colon;
    while (p < end) {
        if (*p == '\'' || *p == '"') {
            /* Copy quoted string verbatim — no rewriting inside quotes */
            char quote = *p;
            const char *q_start = p;
            p++;
            while (p < end) {
                if (*p == quote) {
                    p++;
                    if (p < end && *p == quote) { p++; } /* escaped quote */
                    else break;
                } else { p++; }
            }
            smart_str_appendl(&result, q_start, p - q_start);
        } else if (*p == '/' && (p + 1) < end && p[1] == '*') {
            /* Copy block comment verbatim — no rewriting inside comments */
            const char *c_start = p;
            p += 2;
            while (p + 1 < end && !(*p == '*' && p[1] == '/')) p++;
            if (p + 1 < end) p += 2;
            smart_str_appendl(&result, c_start, p - c_start);
        } else if (*p == '-' && (p + 1) < end && p[1] == '-') {
            /* Copy line comment verbatim — no rewriting inside comments */
            const char *c_start = p;
            p += 2;
            while (p < end && *p != '\n') p++;
            if (p < end) p++; /* include newline */
            smart_str_appendl(&result, c_start, p - c_start);
        } else if (*p == ':' && (p + 1) < end && (isalpha((unsigned char)p[1]) || p[1] == '_')) {
            const char *ns = p + 1;
            const char *ne = ns;
            while (ne < end && (isalnum((unsigned char)*ne) || *ne == '_')) ne++;
            size_t nlen = ne - ns;

            /* Find or add name */
            int idx = -1;
            for (uint32_t i = 0; i < name_count; i++) {
                if (ZSTR_LEN(names[i]) == nlen && memcmp(ZSTR_VAL(names[i]), ns, nlen) == 0) {
                    idx = (int)i;
                    break;
                }
            }
            if (idx == -1) {
                if (name_count >= name_alloc) {
                    name_alloc = name_alloc ? name_alloc * 2 : 8;
                    names = erealloc(names, sizeof(zend_string *) * name_alloc);
                }
                names[name_count] = zend_string_init(ns, nlen, 0);
                idx = (int)name_count;
                name_count++;
            }

            char numbuf[16];
            int numlen = snprintf(numbuf, sizeof(numbuf), "$%d", idx + 1);
            smart_str_appendl(&result, numbuf, numlen);

            p = ne;
        } else {
            /* Bulk-copy run of non-param, non-quote, non-comment characters */
            const char *run_start = p;
            while (p < end && *p != '\'' && *p != '"' &&
                   !(*p == '/' && (p + 1) < end && p[1] == '*') &&
                   !(*p == '-' && (p + 1) < end && p[1] == '-') &&
                   !(*p == ':' && (p + 1) < end &&
                     (isalpha((unsigned char)p[1]) || p[1] == '_'))) {
                p++;
            }
            smart_str_appendl(&result, run_start, p - run_start);
        }
    }

    smart_str_0(&result);

    *out_names = names;
    *out_name_count = name_count;

    char *out = estrndup(ZSTR_VAL(result.s), ZSTR_LEN(result.s));
    smart_str_free(&result);
    return out;
}

/* Reorder associative params to positional using name mapping */
/* Returns SUCCESS or FAILURE (exception thrown). */
static int reorder_named_params(zval *params, zend_string **names, uint32_t name_count,
    zval *out_positional)
{
    array_init_size(out_positional, name_count);
    for (uint32_t i = 0; i < name_count; i++) {
        zval *val = zend_hash_find(Z_ARRVAL_P(params), names[i]);
        if (val) {
            Z_TRY_ADDREF_P(val);
            add_next_index_zval(out_positional, val);
        } else {
            zval_ptr_dtor(out_positional);
            ZVAL_UNDEF(out_positional);
            zend_throw_exception_ex(stoolap_exception_ce, 0,
                "Missing named parameter: :%s", ZSTR_VAL(names[i]));
            return FAILURE;
        }
    }
    return SUCCESS;
}

/* Check if array is a list (sequential integer keys 0..n-1).
 * Uses zend_array_is_list which is O(1) for packed arrays. */
static inline int zval_array_is_list(zval *arr)
{
    return zend_array_is_list(Z_ARRVAL_P(arr));
}

/* Build params using statement's cached array when param count is stable.
 * Returns SUCCESS or FAILURE (exception set). JSON strings need stoolap_stmt_params_free. */
static int stoolap_stmt_build_params(stoolap_stmt_obj *sobj, zval *params, stoolap_params_t *out)
{
    stoolap_params_init(out);

    if (params == NULL || Z_TYPE_P(params) != IS_ARRAY) {
        return SUCCESS;
    }

    HashTable *ht = Z_ARRVAL_P(params);
    int32_t count = zend_hash_num_elements(ht);
    if (count == 0) return SUCCESS;

    /* Reuse cached array if same size */
    StoolapValue *values;
    if (sobj->cached_values && sobj->cached_count == count) {
        values = sobj->cached_values;
    } else {
        if (sobj->cached_values) efree(sobj->cached_values);
        values = emalloc(count * sizeof(StoolapValue));
        sobj->cached_values = values;
        sobj->cached_count = count;
    }

    out->values = values;
    out->count = count;

    int idx = 0;
    zval *val;
    ZEND_HASH_FOREACH_VAL(ht, val) {
        values[idx]._padding = 0;

        if (Z_TYPE_P(val) == IS_NULL) {
            values[idx].value_type = 0;
        } else if (Z_TYPE_P(val) == IS_TRUE) {
            values[idx].value_type = 4;
            values[idx].v.boolean = 1;
        } else if (Z_TYPE_P(val) == IS_FALSE) {
            values[idx].value_type = 4;
            values[idx].v.boolean = 0;
        } else if (Z_TYPE_P(val) == IS_LONG) {
            values[idx].value_type = 1;
            values[idx].v.integer = Z_LVAL_P(val);
        } else if (Z_TYPE_P(val) == IS_DOUBLE) {
            values[idx].value_type = 2;
            values[idx].v.float64 = Z_DVAL_P(val);
        } else if (Z_TYPE_P(val) == IS_STRING) {
            values[idx].value_type = 3;
            values[idx].v.text.ptr = Z_STRVAL_P(val);
            values[idx].v.text.len = Z_STRLEN_P(val);
        } else if (Z_TYPE_P(val) == IS_ARRAY) {
            smart_str buf = {0};
            if (php_json_encode(&buf, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES) == FAILURE) {
                smart_str_free(&buf);
                zend_throw_exception(stoolap_exception_ce,
                    "Failed to JSON-encode array parameter", 0);
                return FAILURE;
            }
            smart_str_0(&buf);
            values[idx].value_type = 6;
            values[idx].v.text.ptr = ZSTR_VAL(buf.s);
            values[idx].v.text.len = ZSTR_LEN(buf.s);
            if (out->json_count >= out->json_alloc) {
                out->json_alloc = out->json_alloc ? out->json_alloc * 2 : 4;
                out->json_strs = erealloc(out->json_strs, sizeof(zend_string *) * out->json_alloc);
            }
            out->json_strs[out->json_count++] = buf.s;
        } else if (Z_TYPE_P(val) == IS_OBJECT) {
            if (datetime_interface_ce && instanceof_function(Z_OBJCE_P(val), datetime_interface_ce)) {
                int64_t nanos;
                if (datetime_to_nanos(val, &nanos) != 0) {
                    return FAILURE;
                }
                values[idx].value_type = 5;
                values[idx].v.timestamp_nanos = nanos;
            } else {
                smart_str buf = {0};
                if (php_json_encode(&buf, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES) == FAILURE) {
                    smart_str_free(&buf);
                    zend_throw_exception(stoolap_exception_ce,
                        "Failed to JSON-encode object parameter", 0);
                    return FAILURE;
                }
                smart_str_0(&buf);
                values[idx].value_type = 6;
                values[idx].v.text.ptr = ZSTR_VAL(buf.s);
                values[idx].v.text.len = ZSTR_LEN(buf.s);
                if (out->json_count >= out->json_alloc) {
                    out->json_alloc = out->json_alloc ? out->json_alloc * 2 : 4;
                    out->json_strs = erealloc(out->json_strs, sizeof(zend_string *) * out->json_alloc);
                }
                out->json_strs[out->json_count++] = buf.s;
            }
        } else {
            zend_throw_exception_ex(stoolap_exception_ce, 0,
                "Unsupported parameter type: %s", zend_zval_type_name(val));
            return FAILURE;
        }
        idx++;
    } ZEND_HASH_FOREACH_END();

    return SUCCESS;
}

/* Free only JSON strings from stoolap_stmt_build_params (values are owned by stmt) */
static void stoolap_stmt_params_free(stoolap_params_t *p) {
    for (int i = 0; i < p->json_count; i++) {
        zend_string_release(p->json_strs[i]);
    }
    if (p->json_strs) efree(p->json_strs);
    /* Do NOT efree values — they're cached on the stmt object */
    memset(p, 0, sizeof(*p));
}

/* Full named param rewrite for Database methods (rewrite SQL + reorder params).
 * Returns SUCCESS or FAILURE (exception thrown on missing named param). */
static int rewrite_named_params_full(const char *sql, size_t sql_len,
    zval *params, char **out_sql, zval *out_params)
{
    if (params == NULL || Z_TYPE_P(params) != IS_ARRAY ||
        zend_hash_num_elements(Z_ARRVAL_P(params)) == 0 ||
        zval_array_is_list(params)) {
        *out_sql = NULL;
        if (out_params) ZVAL_UNDEF(out_params);
        return SUCCESS;
    }

    zend_string **names = NULL;
    uint32_t name_count = 0;
    *out_sql = rewrite_named_params_sql(sql, sql_len, &names, &name_count);

    if (*out_sql && names) {
        int rc = reorder_named_params(params, names, name_count, out_params);
        for (uint32_t i = 0; i < name_count; i++) zend_string_release(names[i]);
        efree(names);
        if (rc != SUCCESS) {
            efree(*out_sql);
            *out_sql = NULL;
            return FAILURE;
        }
    } else {
        /* params is associative (checked at top) but SQL has no named placeholders.
         * Silently using hash iteration order for positional $1/$2/... would cause
         * data corruption — reject explicitly. */
        zend_throw_exception(stoolap_exception_ce,
            "Associative array parameters require named placeholders (:name) in SQL; "
            "use a sequential array for positional ($1, $2, ...) placeholders", 0);
        return FAILURE;
    }
    return SUCCESS;
}

/* ================================================================
 * DSN normalization
 * ================================================================ */

static char *normalize_dsn(const char *dsn, size_t dsn_len)
{
    if (dsn_len == 0 || strcmp(dsn, ":memory:") == 0) {
        return NULL; /* Use stoolap_open_in_memory */
    }
    if (strstr(dsn, "://") != NULL) {
        return NULL; /* Already has scheme, use as-is */
    }
    /* Prepend file:// */
    char *result = emalloc(7 + dsn_len + 1);
    memcpy(result, "file://", 7);
    memcpy(result + 7, dsn, dsn_len);
    result[7 + dsn_len] = '\0';
    return result;
}

/* ================================================================
 * Object create / free handlers
 * ================================================================ */

static zend_object *stoolap_db_create(zend_class_entry *ce)
{
    stoolap_db_obj *obj = zend_object_alloc(sizeof(stoolap_db_obj), ce);
    obj->db = NULL;
    zend_object_std_init(&obj->std, ce);
    obj->std.handlers = &stoolap_database_handlers;
    return &obj->std;
}

static void stoolap_db_free(zend_object *object)
{
    stoolap_db_obj *obj = stoolap_db_from_obj(object);
    if (obj->db) {
        stoolap_close(obj->db);
        obj->db = NULL;
    }
    zend_object_std_dtor(object);
}

static zend_object *stoolap_stmt_create(zend_class_entry *ce)
{
    stoolap_stmt_obj *obj = zend_object_alloc(sizeof(stoolap_stmt_obj), ce);
    obj->stmt = NULL;
    obj->sql = NULL;
    obj->param_names = NULL;
    obj->param_name_count = 0;
    obj->cached_values = NULL;
    obj->cached_count = 0;
    ZVAL_UNDEF(&obj->db_zv);
    zend_object_std_init(&obj->std, ce);
    obj->std.handlers = &stoolap_statement_handlers;
    return &obj->std;
}

static void stoolap_stmt_free(zend_object *object)
{
    stoolap_stmt_obj *obj = stoolap_stmt_from_obj(object);
    if (obj->stmt) {
        stoolap_stmt_finalize(obj->stmt);
        obj->stmt = NULL;
    }
    if (obj->sql) {
        zend_string_release(obj->sql);
        obj->sql = NULL;
    }
    if (obj->param_names) {
        for (uint32_t i = 0; i < obj->param_name_count; i++) {
            zend_string_release(obj->param_names[i]);
        }
        efree(obj->param_names);
        obj->param_names = NULL;
    }
    if (obj->cached_values) {
        efree(obj->cached_values);
        obj->cached_values = NULL;
    }
    zval_ptr_dtor(&obj->db_zv);
    ZVAL_UNDEF(&obj->db_zv);
    zend_object_std_dtor(object);
}

static zend_object *stoolap_tx_create(zend_class_entry *ce)
{
    stoolap_tx_obj *obj = zend_object_alloc(sizeof(stoolap_tx_obj), ce);
    obj->tx = NULL;
    obj->db = NULL;
    ZVAL_UNDEF(&obj->db_zv);
    zend_object_std_init(&obj->std, ce);
    obj->std.handlers = &stoolap_transaction_handlers;
    return &obj->std;
}

static void stoolap_tx_free(zend_object *object)
{
    stoolap_tx_obj *obj = stoolap_tx_from_obj(object);
    if (obj->tx) {
        stoolap_tx_rollback(obj->tx);
        obj->tx = NULL;
    }
    zval_ptr_dtor(&obj->db_zv);
    ZVAL_UNDEF(&obj->db_zv);
    zend_object_std_dtor(object);
}

/* ================================================================
 * Database methods
 * ================================================================ */

PHP_METHOD(Stoolap_Database, open)
{
    char *dsn = NULL;
    size_t dsn_len = 0;

    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING(dsn, dsn_len)
    ZEND_PARSE_PARAMETERS_END();

    StoolapDB *db = NULL;
    int32_t rc;

    if (dsn == NULL || dsn_len == 0 || strcmp(dsn, ":memory:") == 0 || strcmp(dsn, "memory://") == 0) {
        rc = stoolap_open_in_memory(&db);
    } else {
        char *actual_dsn = normalize_dsn(dsn, dsn_len);
        if (actual_dsn) {
            rc = stoolap_open(actual_dsn, &db);
            efree(actual_dsn);
        } else {
            rc = stoolap_open(dsn, &db);
        }
    }

    if (rc != 0) {
        const char *err = stoolap_errmsg(NULL);
        zend_throw_exception(stoolap_exception_ce, err ? err : "Failed to open database", 0);
        RETURN_THROWS();
    }

    object_init_ex(return_value, stoolap_database_ce);
    stoolap_db_obj *obj = Z_STOOLAP_DB_P(return_value);
    obj->db = db;
}

PHP_METHOD(Stoolap_Database, openInMemory)
{
    ZEND_PARSE_PARAMETERS_NONE();

    StoolapDB *db = NULL;
    int32_t rc = stoolap_open_in_memory(&db);
    if (rc != 0) {
        const char *err = stoolap_errmsg(NULL);
        zend_throw_exception(stoolap_exception_ce, err ? err : "Failed to open database", 0);
        RETURN_THROWS();
    }

    object_init_ex(return_value, stoolap_database_ce);
    Z_STOOLAP_DB_P(return_value)->db = db;
}

PHP_METHOD(Stoolap_Database, exec)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *actual_sql = clean_sql ? clean_sql : sql;

    int64_t affected = 0;
    int32_t rc = stoolap_exec(obj->db, actual_sql, &affected);

    if (clean_sql) efree(clean_sql);

    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "exec failed", 0);
        RETURN_THROWS();
    }

    RETURN_LONG((zend_long)affected);
}

PHP_METHOD(Stoolap_Database, execute)
{
    char *sql;
    size_t sql_len;
    zval *params;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_ARRAY(params)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *actual_sql = clean_sql ? clean_sql : sql;

    /* Named param rewrite */
    char *rewritten_sql = NULL;
    zval positional_params;
    ZVAL_UNDEF(&positional_params);
    if (rewrite_named_params_full(actual_sql, clean_sql ? clean_len : sql_len,
                              params, &rewritten_sql, &positional_params) != SUCCESS) {
        if (clean_sql) efree(clean_sql);
        RETURN_THROWS();
    }

    const char *final_sql = rewritten_sql ? rewritten_sql : actual_sql;
    zval *final_params = Z_TYPE(positional_params) != IS_UNDEF ? &positional_params : params;

    stoolap_params_t sp;
    if (stoolap_build_params(final_params, &sp) != SUCCESS) {
        stoolap_params_free(&sp);
        if (rewritten_sql) efree(rewritten_sql);
        if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
        if (clean_sql) efree(clean_sql);
        RETURN_THROWS();
    }

    int64_t affected = 0;
    int32_t rc = stoolap_exec_params(obj->db, final_sql, sp.values, sp.count, &affected);

    stoolap_params_free(&sp);
    if (rewritten_sql) efree(rewritten_sql);
    if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
    if (clean_sql) efree(clean_sql);

    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "execute failed", 0);
        RETURN_THROWS();
    }

    RETURN_LONG((zend_long)affected);
}

PHP_METHOD(Stoolap_Database, executeBatch)
{
    char *sql;
    size_t sql_len;
    zval *params_array;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_ARRAY(params_array)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    HashTable *outer = Z_ARRVAL_P(params_array);
    uint32_t batch_len = zend_hash_num_elements(outer);
    if (batch_len == 0) {
        RETURN_LONG(0);
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *final_sql = clean_sql ? clean_sql : sql;
    size_t final_sql_len = clean_sql ? clean_len : sql_len;

    /* Rewrite named params (:name → $N) in SQL */
    zend_string **param_names = NULL;
    uint32_t param_name_count = 0;
    char *rewritten_sql = rewrite_named_params_sql(final_sql, final_sql_len,
                                                    &param_names, &param_name_count);
    if (rewritten_sql) {
        if (clean_sql) efree(clean_sql);
        clean_sql = rewritten_sql;
        final_sql = clean_sql;
    }

    /* Determine param count from first row and pre-allocate ONCE */
    zval *first_row;
    ZEND_HASH_FOREACH_VAL(outer, first_row) { break; } ZEND_HASH_FOREACH_END();
    int32_t param_count;
    if (Z_TYPE_P(first_row) != IS_ARRAY) {
        param_count = 0;
    } else if (param_names && !zval_array_is_list(first_row)) {
        param_count = (int32_t)param_name_count;
    } else if (Z_TYPE_P(first_row) == IS_ARRAY && !zval_array_is_list(first_row) && !param_names) {
        /* Associative row but no named params in SQL — would silently corrupt data */
        if (clean_sql) efree(clean_sql);
        zend_throw_exception(stoolap_exception_ce,
            "Associative array parameters require named placeholders (:name) in SQL; "
            "use a sequential array for positional ($1, $2, ...) placeholders", 0);
        RETURN_THROWS();
    } else {
        param_count = (int32_t)zend_hash_num_elements(Z_ARRVAL_P(first_row));
    }

    StoolapValue *values = emalloc(param_count * sizeof(StoolapValue));
    int json_alloc = param_count;
    zend_string **json_strs = emalloc(json_alloc * sizeof(zend_string *));
    int json_count = 0;

    /* Begin transaction + prepare statement once */
    StoolapTx *tx = NULL;
    int32_t rc = stoolap_begin(obj->db, &tx);
    if (rc != 0) {
        efree(values); efree(json_strs);
        if (param_names) {
            for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
            efree(param_names);
        }
        if (clean_sql) efree(clean_sql);
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "executeBatch: begin failed", 0);
        RETURN_THROWS();
    }

    StoolapStmt *stmt = NULL;
    rc = stoolap_prepare(obj->db, final_sql, &stmt);
    if (rc != 0) {
        stoolap_tx_rollback(tx);
        efree(values); efree(json_strs);
        if (param_names) {
            for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
            efree(param_names);
        }
        if (clean_sql) efree(clean_sql);
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "executeBatch: prepare failed", 0);
        RETURN_THROWS();
    }

    int64_t total_affected = 0;
    zval *row;
    ZEND_HASH_FOREACH_VAL(outer, row) {
        if (Z_TYPE_P(row) != IS_ARRAY) {
            stoolap_batch_json_free(json_strs, &json_count);
            efree(values); efree(json_strs);
            stoolap_stmt_finalize(stmt);
            stoolap_tx_rollback(tx);
            if (param_names) {
                for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                efree(param_names);
            }
            if (clean_sql) efree(clean_sql);
            zend_throw_exception(stoolap_exception_ce, "executeBatch: each element must be an array", 0);
            RETURN_THROWS();
        }

        /* Reorder associative rows to positional using name mapping */
        zval positional;
        ZVAL_UNDEF(&positional);
        zval *fill_row = row;
        if (param_names && !zval_array_is_list(row)) {
            if (reorder_named_params(row, param_names, param_name_count, &positional) != SUCCESS) {
                stoolap_batch_json_free(json_strs, &json_count);
                efree(values); efree(json_strs);
                stoolap_stmt_finalize(stmt);
                stoolap_tx_rollback(tx);
                if (param_names) {
                    for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                    efree(param_names);
                }
                if (clean_sql) efree(clean_sql);
                RETURN_THROWS();
            }
            fill_row = &positional;
        }

        int fill_rc = stoolap_batch_fill_params(fill_row, values, param_count, json_strs, &json_count, json_alloc);
        if (Z_TYPE(positional) != IS_UNDEF) zval_ptr_dtor(&positional);

        if (fill_rc != SUCCESS) {
            stoolap_batch_json_free(json_strs, &json_count);
            efree(values); efree(json_strs);
            stoolap_stmt_finalize(stmt);
            stoolap_tx_rollback(tx);
            if (param_names) {
                for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                efree(param_names);
            }
            if (clean_sql) efree(clean_sql);
            if (!EG(exception)) {
                zend_throw_exception(stoolap_exception_ce,
                    "executeBatch: parameter count mismatch (all rows must have the same number of parameters)", 0);
            }
            RETURN_THROWS();
        }

        int64_t affected = 0;
        rc = stoolap_tx_stmt_exec(tx, stmt, values, param_count, &affected);

        stoolap_batch_json_free(json_strs, &json_count);

        if (rc != 0) {
            const char *err = stoolap_tx_errmsg(tx);
            /* Throw before rollback — rollback frees tx and its error string */
            zend_throw_exception(stoolap_exception_ce, err ? err : "executeBatch failed", 0);
            efree(values); efree(json_strs);
            stoolap_stmt_finalize(stmt);
            stoolap_tx_rollback(tx);
            if (param_names) {
                for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                efree(param_names);
            }
            if (clean_sql) efree(clean_sql);
            RETURN_THROWS();
        }

        total_affected += affected;
    } ZEND_HASH_FOREACH_END();

    stoolap_stmt_finalize(stmt);
    efree(values);
    efree(json_strs);
    if (param_names) {
        for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
        efree(param_names);
    }

    /* Commit — tx is consumed by commit (even on failure) */
    rc = stoolap_tx_commit(tx);
    if (clean_sql) efree(clean_sql);

    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "executeBatch: commit failed", 0);
        RETURN_THROWS();
    }

    RETURN_LONG((zend_long)total_affected);
}

static void stoolap_db_query_impl(INTERNAL_FUNCTION_PARAMETERS, int mode)
{
    char *sql;
    size_t sql_len;
    zval *params = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *actual_sql = clean_sql ? clean_sql : sql;
    size_t actual_len = clean_sql ? clean_len : sql_len;

    /* For queryOne, auto-inject LIMIT 1 to avoid full-result materialization */
    char *limited_sql = NULL;
    if (mode == QUERY_ONE) {
        size_t lim_len;
        limited_sql = ensure_limit_one(actual_sql, actual_len, &lim_len);
        if (limited_sql) {
            actual_sql = limited_sql;
            actual_len = lim_len;
        }
    }

    StoolapRows *rows = NULL;
    int32_t rc;

    if (params == NULL || Z_TYPE_P(params) != IS_ARRAY || zend_hash_num_elements(Z_ARRVAL_P(params)) == 0) {
        rc = stoolap_query(obj->db, actual_sql, &rows);
    } else {
        /* Named param rewrite */
        char *rewritten_sql = NULL;
        zval positional_params;
        ZVAL_UNDEF(&positional_params);
        if (rewrite_named_params_full(actual_sql, actual_len, params, &rewritten_sql, &positional_params) != SUCCESS) {
            if (limited_sql) efree(limited_sql);
            if (clean_sql) efree(clean_sql);
            RETURN_THROWS();
        }

        const char *final_sql = rewritten_sql ? rewritten_sql : actual_sql;
        zval *final_params = Z_TYPE(positional_params) != IS_UNDEF ? &positional_params : params;

        stoolap_params_t sp;
        if (stoolap_build_params(final_params, &sp) != SUCCESS) {
            stoolap_params_free(&sp);
            if (rewritten_sql) efree(rewritten_sql);
            if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
            if (limited_sql) efree(limited_sql);
            if (clean_sql) efree(clean_sql);
            RETURN_THROWS();
        }

        rc = stoolap_query_params(obj->db, final_sql, sp.values, sp.count, &rows);

        stoolap_params_free(&sp);
        if (rewritten_sql) efree(rewritten_sql);
        if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
    }

    if (limited_sql) efree(limited_sql);
    if (clean_sql) efree(clean_sql);

    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "query failed", 0);
        RETURN_THROWS();
    }

    stoolap_handle_rows(rows, return_value, mode);
}

PHP_METHOD(Stoolap_Database, query) { stoolap_db_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_ASSOC); }
PHP_METHOD(Stoolap_Database, queryOne) { stoolap_db_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_ONE); }
PHP_METHOD(Stoolap_Database, queryRaw) { stoolap_db_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_RAW); }

PHP_METHOD(Stoolap_Database, prepare)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *original_sql = clean_sql ? clean_sql : sql;
    size_t original_len = clean_sql ? clean_len : sql_len;

    /* Rewrite named params */
    zend_string **param_names = NULL;
    uint32_t param_name_count = 0;
    char *rewritten_sql = rewrite_named_params_sql(original_sql, original_len,
                                                    &param_names, &param_name_count);

    const char *prepare_sql = rewritten_sql ? rewritten_sql : original_sql;

    StoolapStmt *stmt = NULL;
    int32_t rc = stoolap_prepare(obj->db, prepare_sql, &stmt);

    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        if (rewritten_sql) efree(rewritten_sql);
        if (clean_sql) efree(clean_sql);
        if (param_names) {
            for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
            efree(param_names);
        }
        zend_throw_exception(stoolap_exception_ce, err ? err : "prepare failed", 0);
        RETURN_THROWS();
    }

    object_init_ex(return_value, stoolap_statement_ce);
    stoolap_stmt_obj *sobj = Z_STOOLAP_STMT_P(return_value);
    sobj->stmt = stmt;
    sobj->sql = zend_string_init(original_sql, original_len, 0);
    sobj->param_names = param_names;
    sobj->param_name_count = param_name_count;
    ZVAL_COPY(&sobj->db_zv, ZEND_THIS);

    if (rewritten_sql) efree(rewritten_sql);
    if (clean_sql) efree(clean_sql);
}

PHP_METHOD(Stoolap_Database, begin)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    StoolapTx *tx = NULL;
    int32_t rc = stoolap_begin(obj->db, &tx);
    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "begin failed", 0);
        RETURN_THROWS();
    }

    object_init_ex(return_value, stoolap_transaction_ce);
    stoolap_tx_obj *tx_obj = Z_STOOLAP_TX_P(return_value);
    tx_obj->tx = tx;
    tx_obj->db = obj->db;
    ZVAL_COPY(&tx_obj->db_zv, ZEND_THIS);
}

PHP_METHOD(Stoolap_Database, beginSnapshot)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    StoolapTx *tx = NULL;
    int32_t rc = stoolap_begin_with_isolation(obj->db, 1, &tx);
    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "beginSnapshot failed", 0);
        RETURN_THROWS();
    }

    object_init_ex(return_value, stoolap_transaction_ce);
    stoolap_tx_obj *tx_obj = Z_STOOLAP_TX_P(return_value);
    tx_obj->tx = tx;
    tx_obj->db = obj->db;
    ZVAL_COPY(&tx_obj->db_zv, ZEND_THIS);
}

PHP_METHOD(Stoolap_Database, clone)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    StoolapDB *new_db = NULL;
    int32_t rc = stoolap_clone(obj->db, &new_db);
    if (rc != 0) {
        const char *err = stoolap_errmsg(obj->db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "clone failed", 0);
        RETURN_THROWS();
    }

    object_init_ex(return_value, stoolap_database_ce);
    Z_STOOLAP_DB_P(return_value)->db = new_db;
}

PHP_METHOD(Stoolap_Database, close)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (obj->db) {
        stoolap_close(obj->db);
        obj->db = NULL;
    }
}

PHP_METHOD(Stoolap_Database, version)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_db_obj *obj = Z_STOOLAP_DB_P(ZEND_THIS);
    if (!obj->db) {
        zend_throw_exception(stoolap_exception_ce, "Database is closed", 0);
        RETURN_THROWS();
    }

    const char *ver = stoolap_version();
    RETURN_STRING(ver ? ver : "");
}

/* ================================================================
 * Statement methods
 * ================================================================ */

/* Check that the underlying Database is still open.
 * Returns 1 if valid, 0 if closed (exception thrown). */
static inline int stoolap_stmt_check_db(stoolap_stmt_obj *sobj)
{
    if (Z_TYPE(sobj->db_zv) == IS_OBJECT) {
        stoolap_db_obj *db_obj = Z_STOOLAP_DB_P(&sobj->db_zv);
        if (db_obj->db) return 1;
    }
    zend_throw_exception(stoolap_exception_ce, "Database has been closed", 0);
    return 0;
}

/* Returns SUCCESS or FAILURE (exception thrown on missing named param). */
static int stmt_normalize_params(stoolap_stmt_obj *sobj, zval *params, zval *out)
{
    if (params == NULL || Z_TYPE_P(params) != IS_ARRAY ||
        zend_hash_num_elements(Z_ARRVAL_P(params)) == 0 ||
        zval_array_is_list(params)) {
        ZVAL_UNDEF(out);
        return SUCCESS;
    }

    /* Named params → positional using stored param_names */
    if (sobj->param_names != NULL && sobj->param_name_count > 0) {
        return reorder_named_params(params, sobj->param_names, sobj->param_name_count, out);
    } else {
        /* params is associative but statement has no named placeholders */
        zend_throw_exception(stoolap_exception_ce,
            "Associative array parameters require named placeholders (:name) in SQL; "
            "use a sequential array for positional ($1, $2, ...) placeholders", 0);
        return FAILURE;
    }
}

PHP_METHOD(Stoolap_Statement, execute)
{
    zval *params = NULL;

    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_stmt_obj *sobj = Z_STOOLAP_STMT_P(ZEND_THIS);
    if (!sobj->stmt) {
        zend_throw_exception(stoolap_exception_ce, "Statement is finalized", 0);
        RETURN_THROWS();
    }
    if (!stoolap_stmt_check_db(sobj)) {
        RETURN_THROWS();
    }

    zval normalized;
    if (stmt_normalize_params(sobj, params, &normalized) != SUCCESS) {
        RETURN_THROWS();
    }
    zval *final_params = Z_TYPE(normalized) != IS_UNDEF ? &normalized : params;

    stoolap_params_t sp;
    if (stoolap_stmt_build_params(sobj, final_params, &sp) != SUCCESS) {
        stoolap_stmt_params_free(&sp);
        if (Z_TYPE(normalized) != IS_UNDEF) zval_ptr_dtor(&normalized);
        RETURN_THROWS();
    }

    int64_t affected = 0;
    int32_t rc = stoolap_stmt_exec(sobj->stmt, sp.values, sp.count, &affected);

    stoolap_stmt_params_free(&sp);
    if (Z_TYPE(normalized) != IS_UNDEF) zval_ptr_dtor(&normalized);

    if (rc != 0) {
        const char *err = stoolap_stmt_errmsg(sobj->stmt);
        zend_throw_exception(stoolap_exception_ce, err ? err : "stmt execute failed", 0);
        RETURN_THROWS();
    }

    RETURN_LONG((zend_long)affected);
}

static void stoolap_stmt_query_impl(INTERNAL_FUNCTION_PARAMETERS, int mode)
{
    zval *params = NULL;

    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_stmt_obj *sobj = Z_STOOLAP_STMT_P(ZEND_THIS);
    if (!sobj->stmt) {
        zend_throw_exception(stoolap_exception_ce, "Statement is finalized", 0);
        RETURN_THROWS();
    }
    if (!stoolap_stmt_check_db(sobj)) {
        RETURN_THROWS();
    }

    zval normalized;
    if (stmt_normalize_params(sobj, params, &normalized) != SUCCESS) {
        RETURN_THROWS();
    }
    zval *final_params = Z_TYPE(normalized) != IS_UNDEF ? &normalized : params;

    stoolap_params_t sp;
    if (stoolap_stmt_build_params(sobj, final_params, &sp) != SUCCESS) {
        stoolap_stmt_params_free(&sp);
        if (Z_TYPE(normalized) != IS_UNDEF) zval_ptr_dtor(&normalized);
        RETURN_THROWS();
    }

    StoolapRows *rows = NULL;
    int32_t rc = stoolap_stmt_query(sobj->stmt, sp.values, sp.count, &rows);

    stoolap_stmt_params_free(&sp);
    if (Z_TYPE(normalized) != IS_UNDEF) zval_ptr_dtor(&normalized);

    if (rc != 0) {
        const char *err = stoolap_stmt_errmsg(sobj->stmt);
        zend_throw_exception(stoolap_exception_ce, err ? err : "stmt query failed", 0);
        RETURN_THROWS();
    }

    stoolap_handle_rows(rows, return_value, mode);
}

PHP_METHOD(Stoolap_Statement, query) { stoolap_stmt_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_ASSOC); }
PHP_METHOD(Stoolap_Statement, queryOne) { stoolap_stmt_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_ONE); }
PHP_METHOD(Stoolap_Statement, queryRaw) { stoolap_stmt_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_RAW); }

PHP_METHOD(Stoolap_Statement, sql)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_stmt_obj *sobj = Z_STOOLAP_STMT_P(ZEND_THIS);
    if (sobj->sql) {
        RETURN_STR_COPY(sobj->sql);
    }
    RETURN_EMPTY_STRING();
}

PHP_METHOD(Stoolap_Statement, finalize)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_stmt_obj *sobj = Z_STOOLAP_STMT_P(ZEND_THIS);
    if (sobj->stmt) {
        stoolap_stmt_finalize(sobj->stmt);
        sobj->stmt = NULL;
    }
}

/* ================================================================
 * Transaction methods
 * ================================================================ */

/* Check that the underlying Database is still open.
 * Returns 1 if valid, 0 if closed (exception thrown). */
static inline int stoolap_tx_check_db(stoolap_tx_obj *obj)
{
    if (Z_TYPE(obj->db_zv) == IS_OBJECT) {
        stoolap_db_obj *db_obj = Z_STOOLAP_DB_P(&obj->db_zv);
        if (db_obj->db) return 1;
    }
    zend_throw_exception(stoolap_exception_ce, "Database has been closed", 0);
    return 0;
}

PHP_METHOD(Stoolap_Transaction, exec)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_tx_obj *obj = Z_STOOLAP_TX_P(ZEND_THIS);
    if (!obj->tx) {
        zend_throw_exception(stoolap_exception_ce, "Transaction is already committed or rolled back", 0);
        RETURN_THROWS();
    }
    if (!stoolap_tx_check_db(obj)) {
        RETURN_THROWS();
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *actual_sql = clean_sql ? clean_sql : sql;

    int64_t affected = 0;
    int32_t rc = stoolap_tx_exec(obj->tx, actual_sql, &affected);

    if (clean_sql) efree(clean_sql);

    if (rc != 0) {
        const char *err = stoolap_tx_errmsg(obj->tx);
        zend_throw_exception(stoolap_exception_ce, err ? err : "tx exec failed", 0);
        RETURN_THROWS();
    }

    RETURN_LONG((zend_long)affected);
}

PHP_METHOD(Stoolap_Transaction, execute)
{
    char *sql;
    size_t sql_len;
    zval *params;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_ARRAY(params)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_tx_obj *obj = Z_STOOLAP_TX_P(ZEND_THIS);
    if (!obj->tx) {
        zend_throw_exception(stoolap_exception_ce, "Transaction is already committed or rolled back", 0);
        RETURN_THROWS();
    }
    if (!stoolap_tx_check_db(obj)) {
        RETURN_THROWS();
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *actual_sql = clean_sql ? clean_sql : sql;
    size_t actual_len = clean_sql ? clean_len : sql_len;

    char *rewritten_sql = NULL;
    zval positional_params;
    ZVAL_UNDEF(&positional_params);
    if (rewrite_named_params_full(actual_sql, actual_len, params, &rewritten_sql, &positional_params) != SUCCESS) {
        if (clean_sql) efree(clean_sql);
        RETURN_THROWS();
    }

    const char *final_sql = rewritten_sql ? rewritten_sql : actual_sql;
    zval *final_params = Z_TYPE(positional_params) != IS_UNDEF ? &positional_params : params;

    stoolap_params_t sp;
    if (stoolap_build_params(final_params, &sp) != SUCCESS) {
        stoolap_params_free(&sp);
        if (rewritten_sql) efree(rewritten_sql);
        if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
        if (clean_sql) efree(clean_sql);
        RETURN_THROWS();
    }

    int64_t affected = 0;
    int32_t rc = stoolap_tx_exec_params(obj->tx, final_sql, sp.values, sp.count, &affected);

    stoolap_params_free(&sp);
    if (rewritten_sql) efree(rewritten_sql);
    if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
    if (clean_sql) efree(clean_sql);

    if (rc != 0) {
        const char *err = stoolap_tx_errmsg(obj->tx);
        zend_throw_exception(stoolap_exception_ce, err ? err : "tx execute failed", 0);
        RETURN_THROWS();
    }

    RETURN_LONG((zend_long)affected);
}

static void stoolap_tx_query_impl(INTERNAL_FUNCTION_PARAMETERS, int mode)
{
    char *sql;
    size_t sql_len;
    zval *params = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_tx_obj *obj = Z_STOOLAP_TX_P(ZEND_THIS);
    if (!obj->tx) {
        zend_throw_exception(stoolap_exception_ce, "Transaction is already committed or rolled back", 0);
        RETURN_THROWS();
    }
    if (!stoolap_tx_check_db(obj)) {
        RETURN_THROWS();
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *actual_sql = clean_sql ? clean_sql : sql;
    size_t actual_len = clean_sql ? clean_len : sql_len;

    /* For queryOne, auto-inject LIMIT 1 to avoid full-result materialization */
    char *limited_sql = NULL;
    if (mode == QUERY_ONE) {
        size_t lim_len;
        limited_sql = ensure_limit_one(actual_sql, actual_len, &lim_len);
        if (limited_sql) {
            actual_sql = limited_sql;
            actual_len = lim_len;
        }
    }

    StoolapRows *rows = NULL;
    int32_t rc;

    if (params == NULL || Z_TYPE_P(params) != IS_ARRAY || zend_hash_num_elements(Z_ARRVAL_P(params)) == 0) {
        rc = stoolap_tx_query(obj->tx, actual_sql, &rows);
    } else {
        char *rewritten_sql = NULL;
        zval positional_params;
        ZVAL_UNDEF(&positional_params);
        if (rewrite_named_params_full(actual_sql, actual_len, params, &rewritten_sql, &positional_params) != SUCCESS) {
            if (limited_sql) efree(limited_sql);
            if (clean_sql) efree(clean_sql);
            RETURN_THROWS();
        }

        const char *final_sql = rewritten_sql ? rewritten_sql : actual_sql;
        zval *final_params = Z_TYPE(positional_params) != IS_UNDEF ? &positional_params : params;

        stoolap_params_t sp;
        if (stoolap_build_params(final_params, &sp) != SUCCESS) {
            stoolap_params_free(&sp);
            if (rewritten_sql) efree(rewritten_sql);
            if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
            if (limited_sql) efree(limited_sql);
            if (clean_sql) efree(clean_sql);
            RETURN_THROWS();
        }

        rc = stoolap_tx_query_params(obj->tx, final_sql, sp.values, sp.count, &rows);

        stoolap_params_free(&sp);
        if (rewritten_sql) efree(rewritten_sql);
        if (Z_TYPE(positional_params) != IS_UNDEF) zval_ptr_dtor(&positional_params);
    }

    if (limited_sql) efree(limited_sql);
    if (clean_sql) efree(clean_sql);

    if (rc != 0) {
        const char *err = stoolap_tx_errmsg(obj->tx);
        zend_throw_exception(stoolap_exception_ce, err ? err : "tx query failed", 0);
        RETURN_THROWS();
    }

    stoolap_handle_rows(rows, return_value, mode);
}

PHP_METHOD(Stoolap_Transaction, query) { stoolap_tx_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_ASSOC); }
PHP_METHOD(Stoolap_Transaction, queryOne) { stoolap_tx_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_ONE); }
PHP_METHOD(Stoolap_Transaction, queryRaw) { stoolap_tx_query_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, QUERY_RAW); }

PHP_METHOD(Stoolap_Transaction, executeBatch)
{
    char *sql;
    size_t sql_len;
    zval *params_array;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_ARRAY(params_array)
    ZEND_PARSE_PARAMETERS_END();

    stoolap_tx_obj *obj = Z_STOOLAP_TX_P(ZEND_THIS);
    if (!obj->tx) {
        zend_throw_exception(stoolap_exception_ce, "Transaction is already committed or rolled back", 0);
        RETURN_THROWS();
    }
    if (!stoolap_tx_check_db(obj)) {
        RETURN_THROWS();
    }

    /* Resolve DB handle for stoolap_prepare */
    stoolap_db_obj *db_obj = Z_STOOLAP_DB_P(&obj->db_zv);
    StoolapDB *db = db_obj->db;

    HashTable *outer = Z_ARRVAL_P(params_array);
    uint32_t batch_len = zend_hash_num_elements(outer);
    if (batch_len == 0) {
        RETURN_LONG(0);
    }

    size_t clean_len;
    char *clean_sql = strip_semicolon(sql, sql_len, &clean_len);
    const char *final_sql = clean_sql ? clean_sql : sql;
    size_t final_sql_len = clean_sql ? clean_len : sql_len;

    /* Rewrite named params (:name -> $N) in SQL */
    zend_string **param_names = NULL;
    uint32_t param_name_count = 0;
    char *rewritten_sql = rewrite_named_params_sql(final_sql, final_sql_len,
                                                    &param_names, &param_name_count);
    if (rewritten_sql) {
        if (clean_sql) efree(clean_sql);
        clean_sql = rewritten_sql;
        final_sql = clean_sql;
    }

    /* Determine param count from first row and pre-allocate ONCE */
    zval *first_row;
    ZEND_HASH_FOREACH_VAL(outer, first_row) { break; } ZEND_HASH_FOREACH_END();
    int32_t param_count;
    if (Z_TYPE_P(first_row) != IS_ARRAY) {
        param_count = 0;
    } else if (param_names && !zval_array_is_list(first_row)) {
        param_count = (int32_t)param_name_count;
    } else if (Z_TYPE_P(first_row) == IS_ARRAY && !zval_array_is_list(first_row) && !param_names) {
        /* Associative row but no named params in SQL — would silently corrupt data */
        if (clean_sql) efree(clean_sql);
        zend_throw_exception(stoolap_exception_ce,
            "Associative array parameters require named placeholders (:name) in SQL; "
            "use a sequential array for positional ($1, $2, ...) placeholders", 0);
        RETURN_THROWS();
    } else {
        param_count = (int32_t)zend_hash_num_elements(Z_ARRVAL_P(first_row));
    }

    StoolapValue *values = emalloc(param_count * sizeof(StoolapValue));
    int json_alloc = param_count;
    zend_string **json_strs = emalloc(json_alloc * sizeof(zend_string *));
    int json_count = 0;

    /* Prepare statement once for the batch */
    StoolapStmt *stmt = NULL;
    int32_t rc = stoolap_prepare(db, final_sql, &stmt);
    if (rc != 0) {
        efree(values); efree(json_strs);
        if (param_names) {
            for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
            efree(param_names);
        }
        if (clean_sql) efree(clean_sql);
        const char *err = stoolap_errmsg(db);
        zend_throw_exception(stoolap_exception_ce, err ? err : "executeBatch: prepare failed", 0);
        RETURN_THROWS();
    }

    int64_t total_affected = 0;
    zval *row;
    ZEND_HASH_FOREACH_VAL(outer, row) {
        if (Z_TYPE_P(row) != IS_ARRAY) {
            stoolap_batch_json_free(json_strs, &json_count);
            efree(values); efree(json_strs);
            stoolap_stmt_finalize(stmt);
            if (param_names) {
                for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                efree(param_names);
            }
            if (clean_sql) efree(clean_sql);
            zend_throw_exception(stoolap_exception_ce, "executeBatch: each element must be an array", 0);
            RETURN_THROWS();
        }

        /* Reorder associative rows to positional using name mapping */
        zval positional;
        ZVAL_UNDEF(&positional);
        zval *fill_row = row;
        if (param_names && !zval_array_is_list(row)) {
            if (reorder_named_params(row, param_names, param_name_count, &positional) != SUCCESS) {
                stoolap_batch_json_free(json_strs, &json_count);
                efree(values); efree(json_strs);
                stoolap_stmt_finalize(stmt);
                if (param_names) {
                    for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                    efree(param_names);
                }
                if (clean_sql) efree(clean_sql);
                RETURN_THROWS();
            }
            fill_row = &positional;
        }

        int fill_rc = stoolap_batch_fill_params(fill_row, values, param_count, json_strs, &json_count, json_alloc);
        if (Z_TYPE(positional) != IS_UNDEF) zval_ptr_dtor(&positional);

        if (fill_rc != SUCCESS) {
            stoolap_batch_json_free(json_strs, &json_count);
            efree(values); efree(json_strs);
            stoolap_stmt_finalize(stmt);
            if (param_names) {
                for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                efree(param_names);
            }
            if (clean_sql) efree(clean_sql);
            if (!EG(exception)) {
                zend_throw_exception(stoolap_exception_ce,
                    "executeBatch: parameter count mismatch (all rows must have the same number of parameters)", 0);
            }
            RETURN_THROWS();
        }

        int64_t affected = 0;
        rc = stoolap_tx_stmt_exec(obj->tx, stmt, values, param_count, &affected);

        stoolap_batch_json_free(json_strs, &json_count);

        if (rc != 0) {
            const char *err = stoolap_tx_errmsg(obj->tx);
            /* Throw before finalize — error string may reference stmt internals */
            zend_throw_exception(stoolap_exception_ce, err ? err : "executeBatch failed", 0);
            efree(values); efree(json_strs);
            stoolap_stmt_finalize(stmt);
            if (param_names) {
                for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
                efree(param_names);
            }
            if (clean_sql) efree(clean_sql);
            RETURN_THROWS();
        }

        total_affected += affected;
    } ZEND_HASH_FOREACH_END();

    stoolap_stmt_finalize(stmt);
    efree(values);
    efree(json_strs);
    if (param_names) {
        for (uint32_t i = 0; i < param_name_count; i++) zend_string_release(param_names[i]);
        efree(param_names);
    }
    if (clean_sql) efree(clean_sql);
    RETURN_LONG((zend_long)total_affected);
}

PHP_METHOD(Stoolap_Transaction, commit)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_tx_obj *obj = Z_STOOLAP_TX_P(ZEND_THIS);
    if (!obj->tx) {
        zend_throw_exception(stoolap_exception_ce, "Transaction is already committed or rolled back", 0);
        RETURN_THROWS();
    }
    if (!stoolap_tx_check_db(obj)) {
        RETURN_THROWS();
    }

    StoolapTx *tx = obj->tx;
    obj->tx = NULL;

    int32_t rc = stoolap_tx_commit(tx);
    if (rc != 0) {
        stoolap_db_obj *db_obj = Z_STOOLAP_DB_P(&obj->db_zv);
        const char *err = db_obj->db ? stoolap_errmsg(db_obj->db) : NULL;
        zend_throw_exception(stoolap_exception_ce, err ? err : "commit failed", 0);
        RETURN_THROWS();
    }
}

PHP_METHOD(Stoolap_Transaction, rollback)
{
    ZEND_PARSE_PARAMETERS_NONE();

    stoolap_tx_obj *obj = Z_STOOLAP_TX_P(ZEND_THIS);
    if (!obj->tx) {
        zend_throw_exception(stoolap_exception_ce, "Transaction is already committed or rolled back", 0);
        RETURN_THROWS();
    }
    if (!stoolap_tx_check_db(obj)) {
        RETURN_THROWS();
    }

    StoolapTx *tx = obj->tx;
    obj->tx = NULL;

    int32_t rc = stoolap_tx_rollback(tx);
    if (rc != 0) {
        stoolap_db_obj *db_obj = Z_STOOLAP_DB_P(&obj->db_zv);
        const char *err = db_obj->db ? stoolap_errmsg(db_obj->db) : NULL;
        zend_throw_exception(stoolap_exception_ce, err ? err : "rollback failed", 0);
        RETURN_THROWS();
    }
}

/* ================================================================
 * Argument info
 * ================================================================ */

/* Database */
ZEND_BEGIN_ARG_INFO_EX(arginfo_stoolap_db_open, 0, 0, 0)
    ZEND_ARG_TYPE_INFO(0, dsn, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_stoolap_db_void, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_db_exec, 0, 1, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_db_execute, 0, 2, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, params, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_db_execute_batch, 0, 2, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, paramsArray, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_db_query, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_db_query_one, 0, 1, IS_ARRAY, 1)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_stoolap_db_prepare, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_db_version, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_db_close, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

/* Statement */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_stmt_execute, 0, 0, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_stmt_query, 0, 0, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_stmt_query_one, 0, 0, IS_ARRAY, 1)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_stmt_sql, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_stmt_finalize, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

/* Transaction */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_tx_exec, 0, 1, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_tx_execute, 0, 2, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, params, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_tx_execute_batch, 0, 2, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, paramsArray, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_tx_query, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_tx_query_one, 0, 1, IS_ARRAY, 1)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_tx_commit, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_stoolap_tx_rollback, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

/* ================================================================
 * Method tables
 * ================================================================ */

static const zend_function_entry stoolap_database_methods[] = {
    PHP_ME(Stoolap_Database, open,            arginfo_stoolap_db_open,      ZEND_ACC_PUBLIC | ZEND_ACC_STATIC)
    PHP_ME(Stoolap_Database, openInMemory,    arginfo_stoolap_db_void,      ZEND_ACC_PUBLIC | ZEND_ACC_STATIC)
    PHP_ME(Stoolap_Database, exec,            arginfo_stoolap_db_exec,      ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, execute,         arginfo_stoolap_db_execute,   ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, executeBatch,    arginfo_stoolap_db_execute_batch, ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, query,           arginfo_stoolap_db_query,     ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, queryOne,        arginfo_stoolap_db_query_one, ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, queryRaw,        arginfo_stoolap_db_query,     ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, prepare,         arginfo_stoolap_db_prepare,   ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, begin,           arginfo_stoolap_db_void,      ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, beginSnapshot,   arginfo_stoolap_db_void,      ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, clone,           arginfo_stoolap_db_void,      ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, close,           arginfo_stoolap_db_close,     ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Database, version,         arginfo_stoolap_db_version,   ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry stoolap_statement_methods[] = {
    PHP_ME(Stoolap_Statement, execute,   arginfo_stoolap_stmt_execute,   ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Statement, query,     arginfo_stoolap_stmt_query,     ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Statement, queryOne,  arginfo_stoolap_stmt_query_one, ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Statement, queryRaw,  arginfo_stoolap_stmt_query,     ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Statement, sql,       arginfo_stoolap_stmt_sql,       ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Statement, finalize,  arginfo_stoolap_stmt_finalize,  ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry stoolap_transaction_methods[] = {
    PHP_ME(Stoolap_Transaction, exec,      arginfo_stoolap_tx_exec,     ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Transaction, execute,      arginfo_stoolap_tx_execute,       ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Transaction, executeBatch, arginfo_stoolap_tx_execute_batch, ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Transaction, query,        arginfo_stoolap_tx_query,         ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Transaction, queryOne,  arginfo_stoolap_tx_query_one,ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Transaction, queryRaw,  arginfo_stoolap_tx_query,    ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Transaction, commit,    arginfo_stoolap_tx_commit,   ZEND_ACC_PUBLIC)
    PHP_ME(Stoolap_Transaction, rollback,  arginfo_stoolap_tx_rollback, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

/* ================================================================
 * Module init
 * ================================================================ */

PHP_MINIT_FUNCTION(stoolap)
{
    zend_class_entry ce;

    /* StoolapException */
    INIT_NS_CLASS_ENTRY(ce, "Stoolap", "StoolapException", NULL);
    stoolap_exception_ce = zend_register_internal_class_ex(&ce, spl_ce_RuntimeException);

    /* Database */
    INIT_NS_CLASS_ENTRY(ce, "Stoolap", "Database", stoolap_database_methods);
    stoolap_database_ce = zend_register_internal_class(&ce);
    stoolap_database_ce->ce_flags |= ZEND_ACC_FINAL | ZEND_ACC_NO_DYNAMIC_PROPERTIES;
    stoolap_database_ce->create_object = stoolap_db_create;

    memcpy(&stoolap_database_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    stoolap_database_handlers.offset = XtOffsetOf(stoolap_db_obj, std);
    stoolap_database_handlers.free_obj = stoolap_db_free;
    stoolap_database_handlers.clone_obj = NULL;

    /* Statement */
    INIT_NS_CLASS_ENTRY(ce, "Stoolap", "Statement", stoolap_statement_methods);
    stoolap_statement_ce = zend_register_internal_class(&ce);
    stoolap_statement_ce->ce_flags |= ZEND_ACC_FINAL | ZEND_ACC_NO_DYNAMIC_PROPERTIES;
    stoolap_statement_ce->create_object = stoolap_stmt_create;

    memcpy(&stoolap_statement_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    stoolap_statement_handlers.offset = XtOffsetOf(stoolap_stmt_obj, std);
    stoolap_statement_handlers.free_obj = stoolap_stmt_free;
    stoolap_statement_handlers.clone_obj = NULL;

    /* Transaction */
    INIT_NS_CLASS_ENTRY(ce, "Stoolap", "Transaction", stoolap_transaction_methods);
    stoolap_transaction_ce = zend_register_internal_class(&ce);
    stoolap_transaction_ce->ce_flags |= ZEND_ACC_FINAL | ZEND_ACC_NO_DYNAMIC_PROPERTIES;
    stoolap_transaction_ce->create_object = stoolap_tx_create;

    memcpy(&stoolap_transaction_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    stoolap_transaction_handlers.offset = XtOffsetOf(stoolap_tx_obj, std);
    stoolap_transaction_handlers.free_obj = stoolap_tx_free;
    stoolap_transaction_handlers.clone_obj = NULL;

    datetime_interface_ce = NULL;

    return SUCCESS;
}

PHP_RINIT_FUNCTION(stoolap)
{
    /* Resolve DateTimeInterface once per request (not available during MINIT) */
    if (!datetime_interface_ce) {
        datetime_interface_ce = zend_lookup_class(
            zend_string_init_interned("DateTimeInterface", sizeof("DateTimeInterface") - 1, 0));
    }

    return SUCCESS;
}

PHP_MINFO_FUNCTION(stoolap)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "stoolap support", "enabled");
    php_info_print_table_row(2, "stoolap extension version", PHP_STOOLAP_VERSION);
    php_info_print_table_end();
}

/* ================================================================
 * Module entry
 * ================================================================ */

zend_module_entry stoolap_module_entry = {
    STANDARD_MODULE_HEADER,
    "stoolap",
    NULL,                   /* functions */
    PHP_MINIT(stoolap),
    NULL,                   /* MSHUTDOWN */
    PHP_RINIT(stoolap),
    NULL,                   /* RSHUTDOWN */
    PHP_MINFO(stoolap),
    PHP_STOOLAP_VERSION,
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_STOOLAP
ZEND_GET_MODULE(stoolap)
#endif
