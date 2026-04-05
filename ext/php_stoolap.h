#ifndef PHP_STOOLAP_H
#define PHP_STOOLAP_H

extern zend_module_entry stoolap_module_entry;
#define phpext_stoolap_ptr &stoolap_module_entry

#define PHP_STOOLAP_VERSION "0.4.0"

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

/* Stoolap FFI — see stoolap_daemon.h for full declarations */

#endif
