# stoolap-php

High-performance PHP driver for [Stoolap](https://github.com/stoolap/stoolap), a modern embedded SQL database with MVCC, time-travel queries, and full ACID compliance.

Built as a native PHP extension (C) for minimal overhead.

[![CI](https://github.com/stoolap/stoolap-php/actions/workflows/ci.yml/badge.svg)](https://github.com/stoolap/stoolap-php/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![PHP](https://img.shields.io/badge/php-%3E%3D8.1-8892BF.svg)](https://www.php.net)

## Installation

### Prebuilt Binaries

Download a package matching your PHP version and platform from [GitHub Releases](https://github.com/stoolap/stoolap-php/releases). Each archive contains both the PHP extension and the stoolap library.

```bash
tar xzf stoolap-v0.4.0-php8.4-linux-x86_64.tar.gz
cd stoolap-v0.4.0-php8.4-linux-x86_64

# Install the shared library
sudo cp libstoolap.so /usr/local/lib/
sudo ldconfig  # Linux only

# Install the PHP extension
sudo cp stoolap.so $(php-config --extension-dir)/
```

### Using PIE

Requires the stoolap shared library (`libstoolap.so` / `libstoolap.dylib`) installed on your system. Download it from [Stoolap releases](https://github.com/stoolap/stoolap/releases) or [build from source](https://github.com/stoolap/stoolap).

```bash
pie install stoolap/stoolap-php
```

### From Source

Requires a C compiler and the stoolap shared library.

```bash
cd ext
phpize
./configure --with-stoolap=/path/to/libstoolap
make
sudo make install
```

On Windows, use the PHP SDK build tools:

```cmd
cd ext
phpize.bat
configure --with-stoolap=C:\path\to\libstoolap
nmake
```

### Enable the Extension

```ini
; php.ini or conf.d/stoolap.ini
extension=stoolap
```

Or load it per-invocation:

```bash
php -d extension=stoolap.so your_script.php
```

## Quick Start

```php
use Stoolap\Database;

$db = Database::open(':memory:');

$db->exec('
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    )
');

// Insert with positional parameters ($1, $2, ...)
$db->execute(
    'INSERT INTO users (id, name, email) VALUES ($1, $2, $3)',
    [1, 'Alice', 'alice@example.com']
);

// Insert with named parameters (:key)
$db->execute(
    'INSERT INTO users (id, name, email) VALUES (:id, :name, :email)',
    ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com']
);

// Query rows as associative arrays
$users = $db->query('SELECT * FROM users ORDER BY id');
// [['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'], ...]

// Query single row (LIMIT 1 is auto-injected when not present)
$user = $db->queryOne('SELECT * FROM users WHERE id = $1', [1]);
// ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']

// Query in raw columnar format (faster, no per-row key creation)
$raw = $db->queryRaw('SELECT id, name FROM users ORDER BY id');
// ['columns' => ['id', 'name'], 'rows' => [[1, 'Alice'], [2, 'Bob']]]

$db->close();
```

## API

### Database

```php
// In-memory
$db = Database::open(':memory:');
$db = Database::open('');
$db = Database::openInMemory();

// File-based (data persists across restarts)
$db = Database::open('./mydata');
$db = Database::open('file:///absolute/path/to/db');
```

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `Database::open($dsn)` | `Database` | Open a database |
| `Database::openInMemory()` | `Database` | Open an in-memory database |
| `exec($sql)` | `int` | Execute DDL/DML, returns affected rows |
| `execute($sql, $params)` | `int` | Execute parameterized DML, returns affected rows |
| `executeBatch($sql, $paramsArray)` | `int` | Execute with multiple param sets in a transaction |
| `query($sql, $params?)` | `array` | Query rows as associative arrays |
| `queryOne($sql, $params?)` | `?array` | Query first row or null |
| `queryRaw($sql, $params?)` | `array` | Query in columnar format |
| `prepare($sql)` | `Statement` | Create a prepared statement |
| `begin()` | `Transaction` | Begin a read-write transaction |
| `beginSnapshot()` | `Transaction` | Begin a snapshot (read) transaction |
| `clone()` | `Database` | Clone the database handle |
| `close()` | `void` | Close the database |
| `version()` | `string` | Get stoolap engine version |

`queryOne()` automatically appends `LIMIT 1` when the SQL has no LIMIT clause. For prepared statements, add `LIMIT 1` yourself since the SQL is fixed at prepare time.

#### Batch Execution

`executeBatch()` executes the same SQL with multiple parameter sets in a single atomic transaction. SQL is parsed once and reused for every row -- significantly faster than calling `execute()` in a loop.

```php
$db->executeBatch(
    'INSERT INTO users (id, name, email) VALUES ($1, $2, $3)',
    [
        [1, 'Alice', 'alice@example.com'],
        [2, 'Bob', 'bob@example.com'],
        [3, 'Charlie', 'charlie@example.com'],
    ]
);
// Returns total affected rows (3)
```

On error, all changes are rolled back (atomic). Also available on transactions via `$tx->executeBatch()`.

#### Persistence

File-based databases persist data to disk using WAL (Write-Ahead Logging) and an immutable volume-based storage engine. Hot data lives in a mutable buffer, cold data is sealed into columnar `.vol` files with zone maps, bloom filters, dictionary encoding, and LZ4 compression. Compressed blocks are loaded into RAM at startup; columns are decompressed on first access. Data survives process restarts.

```php
$db = Database::open('./mydata');

$db->exec('CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)');
$db->execute('INSERT INTO kv VALUES ($1, $2)', ['hello', 'world']);
$db->close();

// Reopen - data is still there
$db2 = Database::open('./mydata');
$row = $db2->queryOne('SELECT * FROM kv WHERE key = $1', ['hello']);
// ['key' => 'hello', 'value' => 'world']
$db2->close();
```

##### Configuration

Pass configuration as query parameters in the path:

```php
// Maximum durability: fsync on every write
$db = Database::open('./mydata?sync_mode=full');

// High throughput: no fsync, data durable at checkpoint
$db = Database::open('./mydata?sync_mode=none');

// Custom checkpoint interval with compression
$db = Database::open('./mydata?checkpoint_interval=60&compression=on');

// Multiple options
$db = Database::open(
    './mydata?sync_mode=normal&checkpoint_interval=120&compact_threshold=4'
);
```

##### Sync Modes

Controls the durability vs. performance trade-off:

| Mode | Value | Description |
|------|-------|-------------|
| `none` | `sync_mode=none` | No fsync. Data durable only after checkpoint |
| `normal` | `sync_mode=normal` | Fsync every 1 second (batched). DDL fsyncs immediately (default) |
| `full` | `sync_mode=full` | Fsync on every write. Maximum durability |

##### All Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sync_mode` | `normal` | Sync mode: `none`, `normal`, or `full` |
| `checkpoint_interval` | `60` | Seconds between checkpoint cycles (seal + compact + WAL truncate) |
| `compact_threshold` | `4` | Sub-target volumes per table before merging |
| `target_volume_rows` | `1048576` | Target rows per cold volume. Controls compaction split boundary |
| `checkpoint_on_close` | `on` | Seal all hot rows on clean shutdown for fast startup |
| `wal_compression` | `on` | LZ4 compression for WAL entries |
| `volume_compression` | `on` | LZ4 compression for cold volume files |
| `compression` | `on` | Shorthand: set both `wal_compression` and `volume_compression` |
| `keep_snapshots` | `5` | Number of backup snapshot files to retain |

#### Raw Query Format

`queryRaw()` returns `['columns' => [...], 'rows' => [[...], ...]]` instead of an array of associative arrays. Faster when you don't need named keys.

```php
$raw = $db->queryRaw('SELECT id, name, email FROM users ORDER BY id');
// $raw['columns'] => ['id', 'name', 'email']
// $raw['rows']    => [[1, 'Alice', 'alice@example.com'], [2, 'Bob', 'bob@example.com']]
```

### PreparedStatement

Prepared statements parse SQL once and reuse the execution plan on every call. No parsing overhead per execution.

```php
$insert = $db->prepare('INSERT INTO users VALUES ($1, $2, $3)');
$insert->execute([1, 'Alice', 'alice@example.com']);
$insert->execute([2, 'Bob', 'bob@example.com']);

$lookup = $db->prepare('SELECT * FROM users WHERE id = $1');
$user = $lookup->queryOne([1]);
// ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']
```

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `execute($params?)` | `int` | Execute DML, returns affected rows |
| `query($params?)` | `array` | Query rows as associative arrays |
| `queryOne($params?)` | `?array` | Query single row or null |
| `queryRaw($params?)` | `array` | Query in columnar format |
| `sql()` | `string` | Get the SQL text |
| `finalize()` | `void` | Release the prepared statement |

Statements are automatically finalized on garbage collection.

### Transaction

```php
$tx = $db->begin();
try {
    $tx->execute(
        'INSERT INTO users VALUES ($1, $2, $3)',
        [1, 'Alice', 'alice@example.com']
    );
    $tx->execute(
        'INSERT INTO users VALUES ($1, $2, $3)',
        [2, 'Bob', 'bob@example.com']
    );

    // Read within the transaction (sees uncommitted changes)
    $rows = $tx->query('SELECT * FROM users');
    $one = $tx->queryOne('SELECT * FROM users WHERE id = $1', [1]);
    $raw = $tx->queryRaw('SELECT id, name FROM users');

    $tx->commit();
} catch (\Exception $e) {
    $tx->rollback();
    throw $e;
}
```

Transactions auto-rollback on garbage collection if not committed.

#### Batch in Transaction

```php
$tx = $db->begin();
$tx->executeBatch(
    'INSERT INTO users VALUES ($1, $2, $3)',
    [
        [1, 'Alice', 'alice@example.com'],
        [2, 'Bob', 'bob@example.com'],
    ]
);
$tx->commit(); // or $tx->rollback() to undo
```

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `exec($sql)` | `int` | Execute DDL/DML without params |
| `execute($sql, $params)` | `int` | Execute parameterized DML |
| `executeBatch($sql, $paramsArray)` | `int` | Execute with multiple param sets |
| `query($sql, $params?)` | `array` | Query rows as associative arrays |
| `queryOne($sql, $params?)` | `?array` | Query single row or null |
| `queryRaw($sql, $params?)` | `array` | Query in columnar format |
| `commit()` | `void` | Commit the transaction |
| `rollback()` | `void` | Rollback the transaction |

### Parameters

Both positional and named parameters are supported across all methods:

```php
// Positional ($1, $2, ...)
$db->query('SELECT * FROM users WHERE id = $1 AND name = $2', [1, 'Alice']);

// Named (:key)
$db->query(
    'SELECT * FROM users WHERE id = :id AND name = :name',
    ['id' => 1, 'name' => 'Alice']
);
```

### Error Handling

All methods throw `Stoolap\StoolapException` (extends `\RuntimeException`) on errors:

```php
use Stoolap\StoolapException;

try {
    $db->execute('INSERT INTO users VALUES ($1, $2)', [1, null]); // NOT NULL violation
} catch (StoolapException $e) {
    echo $e->getMessage();
}
```

### Supported Types

| PHP | Stoolap | Notes |
|-----|---------|-------|
| `int` | `INTEGER` | |
| `float` | `FLOAT` | |
| `string` | `TEXT` | |
| `bool` | `BOOLEAN` | |
| `null` | `NULL` | |
| `array` / `object` | `JSON` | Auto-encoded/decoded |
| `DateTimeInterface` | `TIMESTAMP` | Converted to nanoseconds |

## PHP-FPM / Web Server Support

Stoolap uses exclusive file locking, so only one OS process can open a database at a time. The PHP extension solves this transparently for multi-process environments (php-fpm, Apache mod_php) with a built-in daemon proxy.

### How It Works

When loaded under php-fpm, CGI, or Apache, the extension automatically forks a background daemon process on first load. PHP workers communicate with the daemon via shared memory + futex/ulock for near-zero IPC overhead:

- **Shared memory**: 8MB per connection, zero-copy request/response buffers
- **futex (Linux) / __ulock (macOS)**: single-syscall wakeup, ~0.5μs bare roundtrip
- **Spin-then-wait**: fast spin phase catches sub-microsecond responses, kernel wait for longer queries without CPU burn
- **Auto-lifecycle**: daemon starts with php-fpm, exits when php-fpm stops
- **Zero configuration**: works out of the box, no setup required

### IPC Overhead

| Operation | Direct (CLI) | Daemon (FPM) | Overhead |
|---|---|---|---|
| SELECT by PK | 0.5 μs | 1.0 μs | ~0.5 μs |
| UPDATE by PK | 0.8 μs | 1.2 μs | ~0.5 μs |
| Prepared execute | 0.6 μs | 1.1 μs | ~0.5 μs |
| Prepared queryOne | 1.1 μs | 1.7 μs | ~0.6 μs |

Analytical queries (GROUP BY, JOIN, window functions) are unaffected since they're dominated by engine time.

### Environment Variables

| Variable | Values | Description |
|---|---|---|
| `STOOLAP_DAEMON` | `1` / `on` | Force daemon mode (useful for testing) |
| `STOOLAP_DAEMON` | `0` / `off` | Force direct mode (disable daemon) |
| `STOOLAP_DAEMON_DEBUG` | `1` | Enable daemon stderr logging |

When not set, daemon mode is auto-detected from the SAPI name (fpm, cgi, apache).

## Benchmark

Run the included benchmark (Stoolap vs SQLite):

```bash
php -d extension=ext/modules/stoolap.so benchmark.php
```

## Building from Source

Requires:
- PHP >= 8.1 with development headers (`php-dev` / `php-devel`)
- C compiler (gcc, clang, or MSVC)
- The stoolap shared library, either from [Stoolap releases](https://github.com/stoolap/stoolap/releases) or built from source

```bash
git clone https://github.com/stoolap/stoolap-php.git
cd stoolap-php

# Build the extension
cd ext
phpize
./configure --with-stoolap=/path/to/libstoolap
make

# Run tests
cd ..
composer install
php -d extension=ext/modules/stoolap.so vendor/bin/phpunit --testdox
```

## License

Apache 2.0 - see [LICENSE](LICENSE) for details.
