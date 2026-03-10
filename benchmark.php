<?php

// Stoolap vs SQLite benchmark (PHP)
// Both drivers use synchronous methods for fair comparison.
// Both use autocommit -each write is an implicit transaction + commit.
// Matches examples/benchmark.rs test set and ordering.
//
// Run:  php benchmark.php

declare(strict_types=1);

use Stoolap\Database as StoolapDB;

const ROW_COUNT = 10_000;
const ITERATIONS = 500;        // Point queries
const ITERATIONS_MEDIUM = 250; // Index scans, aggregations
const ITERATIONS_HEAVY = 50;   // Full scans, JOINs
const WARMUP = 10;

// ============================================================
// Helpers
// ============================================================

function fmtUs(float $us): string
{
    return str_pad(number_format($us, 3), 15, ' ', STR_PAD_LEFT);
}

$stoolapWins = 0;
$sqliteWins = 0;

function fmtRatio(float $stoolapUs, float $sqliteUs): string
{
    if ($stoolapUs <= 0 || $sqliteUs <= 0) return '      -';
    $ratio = $sqliteUs / $stoolapUs;
    if ($ratio >= 1) {
        return str_pad(number_format($ratio, 2) . 'x', 10, ' ', STR_PAD_LEFT);
    } else {
        return str_pad(number_format(1 / $ratio, 2) . 'x', 9, ' ', STR_PAD_LEFT) . '*';
    }
}

function printRow(string $name, float $stoolapUs, float $sqliteUs): void
{
    global $stoolapWins, $sqliteWins;
    $ratio = fmtRatio($stoolapUs, $sqliteUs);
    if ($stoolapUs < $sqliteUs) $stoolapWins++;
    elseif ($sqliteUs < $stoolapUs) $sqliteWins++;
    echo str_pad($name, 28) . ' | ' . fmtUs($stoolapUs) . ' | ' . fmtUs($sqliteUs) . ' | ' . $ratio . "\n";
}

function printHeader(string $section): void
{
    echo "\n";
    echo str_repeat('=', 80) . "\n";
    echo $section . "\n";
    echo str_repeat('=', 80) . "\n";
    echo str_pad('Operation', 28) . ' | ' . str_pad('Stoolap (μs)', 15, ' ', STR_PAD_LEFT)
        . ' | ' . str_pad('SQLite (μs)', 15, ' ', STR_PAD_LEFT) . ' | ' . str_pad('Ratio', 10, ' ', STR_PAD_LEFT) . "\n";
    echo str_repeat('-', 80) . "\n";
}

function seedRandom(int $i): int
{
    return ($i * 1103515245 + 12345) & 0x7fffffff;
}

function nowUs(): float
{
    return hrtime(true) / 1_000.0; // nanoseconds → microseconds
}

// ============================================================
// Setup databases
// ============================================================

echo "Stoolap vs SQLite (ext-sqlite3) - PHP Benchmark\n";
echo "Configuration: " . ROW_COUNT . " rows, " . ITERATIONS . " iterations per test\n";
echo "All operations are synchronous -fair comparison\n";
echo "Ratio > 1x = Stoolap faster  |  * = SQLite faster\n\n";

// --- Stoolap setup ---
$sdb = StoolapDB::open(':memory:');
$sdb->exec('
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      age INTEGER NOT NULL,
      balance FLOAT NOT NULL,
      active BOOLEAN NOT NULL,
      created_at TEXT NOT NULL
    )
');
$sdb->exec('CREATE INDEX idx_users_age ON users(age)');
$sdb->exec('CREATE INDEX idx_users_active ON users(active)');

// --- SQLite setup ---
$ldb = new SQLite3(':memory:');
$ldb->exec('PRAGMA journal_mode = WAL');
$ldb->exec('
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      age INTEGER NOT NULL,
      balance REAL NOT NULL,
      active INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )
');
$ldb->exec('CREATE INDEX idx_users_age ON users(age)');
$ldb->exec('CREATE INDEX idx_users_active ON users(active)');

// --- Populate users ---
$sInsert = $sdb->prepare('INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)');
$lInsert = $ldb->prepare('INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)');

$ldb->exec('BEGIN');
for ($i = 1; $i <= ROW_COUNT; $i++) {
    $age = (seedRandom($i) % 62) + 18;
    $balance = (seedRandom($i * 7) % 100000) + (seedRandom($i * 13) % 100) / 100;
    $active = seedRandom($i * 3) % 10 < 7 ? 1 : 0;
    $name = "User_{$i}";
    $email = "user{$i}@example.com";

    $sInsert->execute([$i, $name, $email, $age, $balance, $active, '2024-01-01 00:00:00']);

    $lInsert->bindValue(1, $i, SQLITE3_INTEGER);
    $lInsert->bindValue(2, $name, SQLITE3_TEXT);
    $lInsert->bindValue(3, $email, SQLITE3_TEXT);
    $lInsert->bindValue(4, $age, SQLITE3_INTEGER);
    $lInsert->bindValue(5, $balance, SQLITE3_FLOAT);
    $lInsert->bindValue(6, $active, SQLITE3_INTEGER);
    $lInsert->bindValue(7, '2024-01-01 00:00:00', SQLITE3_TEXT);
    $lInsert->execute()->finalize();
}
$ldb->exec('COMMIT');

// ============================================================
// CORE OPERATIONS (matches benchmark.rs section 1)
// ============================================================
printHeader('CORE OPERATIONS');

// --- SELECT by ID ---
{
    $sSt = $sdb->prepare('SELECT * FROM users WHERE id = $1');
    $lSt = $ldb->prepare('SELECT * FROM users WHERE id = ?');

    for ($i = 0; $i < WARMUP; $i++) {
        $id = ($i % ROW_COUNT) + 1;
        $sSt->query([$id]);
        $lSt->bindValue(1, $id, SQLITE3_INTEGER);
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->queryOne([($i % ROW_COUNT) + 1]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $id = ($i % ROW_COUNT) + 1;
        $lSt->bindValue(1, $id, SQLITE3_INTEGER);
        $res = $lSt->execute();
        $res->fetchArray(SQLITE3_ASSOC);
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('SELECT by ID', $sUs, $lUs);
}

// --- SELECT by index (exact) ---
{
    $sSt = $sdb->prepare('SELECT * FROM users WHERE age = $1');
    $lSt = $ldb->prepare('SELECT * FROM users WHERE age = ?');

    for ($i = 0; $i < WARMUP; $i++) {
        $age = ($i % 62) + 18;
        $sSt->query([$age]);
        $lSt->bindValue(1, $age, SQLITE3_INTEGER);
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query([($i % 62) + 18]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $age = ($i % 62) + 18;
        $lSt->bindValue(1, $age, SQLITE3_INTEGER);
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('SELECT by index (exact)', $sUs, $lUs);
}

// --- SELECT by index (range) ---
{
    $sSt = $sdb->prepare('SELECT * FROM users WHERE age >= $1 AND age <= $2');
    $lSt = $ldb->prepare('SELECT * FROM users WHERE age >= ? AND age <= ?');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query([30, 40]);
        $lSt->bindValue(1, 30, SQLITE3_INTEGER);
        $lSt->bindValue(2, 40, SQLITE3_INTEGER);
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query([30, 40]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $lSt->bindValue(1, 30, SQLITE3_INTEGER);
        $lSt->bindValue(2, 40, SQLITE3_INTEGER);
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('SELECT by index (range)', $sUs, $lUs);
}

// --- SELECT complex ---
{
    $sSt = $sdb->prepare('SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100');
    $lSt = $ldb->prepare('SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = 1 ORDER BY balance DESC LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('SELECT complex', $sUs, $lUs);
}

// --- SELECT * (full scan) ---
{
    $sSt = $sdb->prepare('SELECT * FROM users');
    $lSt = $ldb->prepare('SELECT * FROM users');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->queryRaw();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $sSt->queryRaw();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_NUM)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    printRow('SELECT * (full scan)', $sUs, $lUs);
}

// --- UPDATE by ID ---
{
    $sSt = $sdb->prepare('UPDATE users SET balance = $1 WHERE id = $2');
    $lSt = $ldb->prepare('UPDATE users SET balance = ? WHERE id = ?');

    for ($i = 0; $i < WARMUP; $i++) {
        $bal = (seedRandom($i * 17) % 100000) + 0.5;
        $id = ($i % ROW_COUNT) + 1;
        $sSt->execute([$bal, $id]);
        $lSt->bindValue(1, $bal, SQLITE3_FLOAT);
        $lSt->bindValue(2, $id, SQLITE3_INTEGER);
        $lSt->execute();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->execute([(seedRandom($i * 17) % 100000) + 0.5, ($i % ROW_COUNT) + 1]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $lSt->bindValue(1, (seedRandom($i * 17) % 100000) + 0.5, SQLITE3_FLOAT);
        $lSt->bindValue(2, ($i % ROW_COUNT) + 1, SQLITE3_INTEGER);
        $lSt->execute();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('UPDATE by ID', $sUs, $lUs);
}

// --- UPDATE complex ---
{
    $sSt = $sdb->prepare('UPDATE users SET balance = $1 WHERE age >= $2 AND age <= $3 AND active = true');
    $lSt = $ldb->prepare('UPDATE users SET balance = ? WHERE age >= ? AND age <= ? AND active = 1');

    for ($i = 0; $i < WARMUP; $i++) {
        $bal = (seedRandom($i * 23) % 100000) + 0.5;
        $sSt->execute([$bal, 27, 28]);
        $lSt->bindValue(1, $bal, SQLITE3_FLOAT);
        $lSt->bindValue(2, 27, SQLITE3_INTEGER);
        $lSt->bindValue(3, 28, SQLITE3_INTEGER);
        $lSt->execute();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->execute([(seedRandom($i * 23) % 100000) + 0.5, 27, 28]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $lSt->bindValue(1, (seedRandom($i * 23) % 100000) + 0.5, SQLITE3_FLOAT);
        $lSt->bindValue(2, 27, SQLITE3_INTEGER);
        $lSt->bindValue(3, 28, SQLITE3_INTEGER);
        $lSt->execute();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('UPDATE complex', $sUs, $lUs);
}

// --- INSERT single ---
{
    $sSt = $sdb->prepare('INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)');
    $lSt = $ldb->prepare('INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)');
    $base = ROW_COUNT + 1000;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $id = $base + $i;
        $sSt->execute([$id, "New_{$id}", "new{$id}@example.com", (seedRandom($i * 29) % 62) + 18, 100.0, 1, '2024-01-01 00:00:00']);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $id = $base + ITERATIONS + $i;
        $lSt->bindValue(1, $id, SQLITE3_INTEGER);
        $lSt->bindValue(2, "New_{$id}", SQLITE3_TEXT);
        $lSt->bindValue(3, "new{$id}@example.com", SQLITE3_TEXT);
        $lSt->bindValue(4, (seedRandom($i * 29) % 62) + 18, SQLITE3_INTEGER);
        $lSt->bindValue(5, 100.0, SQLITE3_FLOAT);
        $lSt->bindValue(6, 1, SQLITE3_INTEGER);
        $lSt->bindValue(7, '2024-01-01 00:00:00', SQLITE3_TEXT);
        $lSt->execute();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('INSERT single', $sUs, $lUs);
}

// --- DELETE by ID ---
{
    $sSt = $sdb->prepare('DELETE FROM users WHERE id = $1');
    $lSt = $ldb->prepare('DELETE FROM users WHERE id = ?');
    $base = ROW_COUNT + 1000;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->execute([$base + $i]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $lSt->bindValue(1, $base + ITERATIONS + $i, SQLITE3_INTEGER);
        $lSt->execute();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('DELETE by ID', $sUs, $lUs);
}

// --- DELETE complex ---
{
    $sSt = $sdb->prepare('DELETE FROM users WHERE age >= $1 AND age <= $2 AND active = true');
    $lSt = $ldb->prepare('DELETE FROM users WHERE age >= ? AND age <= ? AND active = 1');

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->execute([25, 26]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $lSt->bindValue(1, 25, SQLITE3_INTEGER);
        $lSt->bindValue(2, 26, SQLITE3_INTEGER);
        $lSt->execute();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('DELETE complex', $sUs, $lUs);
}

// --- Aggregation (GROUP BY) ---
{
    $sSt = $sdb->prepare('SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age');
    $lSt = $ldb->prepare('SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    printRow('Aggregation (GROUP BY)', $sUs, $lUs);
}

// ============================================================
// ADVANCED OPERATIONS (matches benchmark.rs section 2)
// ============================================================

// Create orders table
$sdb->exec('
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      amount FLOAT NOT NULL,
      status TEXT NOT NULL,
      order_date TEXT NOT NULL
    )
');
$sdb->exec('CREATE INDEX idx_orders_user_id ON orders(user_id)');
$sdb->exec('CREATE INDEX idx_orders_status ON orders(status)');

$ldb->exec('
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      amount REAL NOT NULL,
      status TEXT NOT NULL,
      order_date TEXT NOT NULL
    )
');
$ldb->exec('CREATE INDEX idx_orders_user_id ON orders(user_id)');
$ldb->exec('CREATE INDEX idx_orders_status ON orders(status)');

// Populate orders (3 per user on average)
$sOrderInsert = $sdb->prepare('INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)');
$lOrderInsert = $ldb->prepare('INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)');
$statuses = ['pending', 'completed', 'shipped', 'cancelled'];

$ldb->exec('BEGIN');
for ($i = 1; $i <= ROW_COUNT * 3; $i++) {
    $userId = (seedRandom($i * 11) % ROW_COUNT) + 1;
    $amount = (seedRandom($i * 19) % 990) + 10 + (seedRandom($i * 23) % 100) / 100;
    $status = $statuses[seedRandom($i * 31) % 4];

    $sOrderInsert->execute([$i, $userId, $amount, $status, '2024-01-15']);

    $lOrderInsert->bindValue(1, $i, SQLITE3_INTEGER);
    $lOrderInsert->bindValue(2, $userId, SQLITE3_INTEGER);
    $lOrderInsert->bindValue(3, $amount, SQLITE3_FLOAT);
    $lOrderInsert->bindValue(4, $status, SQLITE3_TEXT);
    $lOrderInsert->bindValue(5, '2024-01-15', SQLITE3_TEXT);
    $lOrderInsert->execute();
}
$ldb->exec('COMMIT');

printHeader('ADVANCED OPERATIONS');

// --- INNER JOIN ---
{
    $sql = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);
    $iters = 100;

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < $iters; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / $iters;

    $t = nowUs();
    for ($i = 0; $i < $iters; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / $iters;

    printRow('INNER JOIN', $sUs, $lUs);
}

// --- LEFT JOIN + GROUP BY ---
{
    $sql = 'SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100';
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);
    $iters = 100;

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < $iters; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / $iters;

    $t = nowUs();
    for ($i = 0; $i < $iters; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / $iters;

    printRow('LEFT JOIN + GROUP BY', $sUs, $lUs);
}

// --- Scalar subquery ---
{
    $sql = "SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Scalar subquery', $sUs, $lUs);
}

// --- IN subquery ---
{
    $sSt = $sdb->prepare("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100");
    $lSt = $ldb->prepare("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    printRow('IN subquery', $sUs, $lUs);
}

// --- EXISTS subquery ---
{
    $sSt = $sdb->prepare("SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'completed') LIMIT 100");
    $lSt = $ldb->prepare("SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'completed') LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    printRow('EXISTS subquery', $sUs, $lUs);
}

// --- CTE + JOIN ---
{
    $sql = "WITH active_users AS (SELECT id, name, balance FROM users WHERE active = true) SELECT au.name, o.amount FROM active_users au INNER JOIN orders o ON au.id = o.user_id LIMIT 100";
    $sSt = $sdb->prepare($sql);
    $lSql = "WITH active_users AS (SELECT id, name, balance FROM users WHERE active = 1) SELECT au.name, o.amount FROM active_users au INNER JOIN orders o ON au.id = o.user_id LIMIT 100";
    $lSt = $ldb->prepare($lSql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    printRow('CTE + JOIN', $sUs, $lUs);
}

// --- Window: ROW_NUMBER ---
{
    $sql = 'SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100';
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Window ROW_NUMBER', $sUs, $lUs);
}

// --- Window: ROW_NUMBER PK ---
{
    $sql = 'SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM users LIMIT 100';
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Window ROW_NUMBER PK', $sUs, $lUs);
}

// --- Window: PARTITION BY ---
{
    $sql = "SELECT name, age, ROW_NUMBER() OVER (PARTITION BY age ORDER BY balance DESC) as rank_in_age FROM users LIMIT 100";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Window PARTITION BY', $sUs, $lUs);
}

// --- UNION ALL ---
{
    $sSt = $sdb->prepare("SELECT name, balance FROM users WHERE age < 25 UNION ALL SELECT name, balance FROM users WHERE age > 60 LIMIT 100");
    $lSt = $ldb->prepare("SELECT name, balance FROM users WHERE age < 25 UNION ALL SELECT name, balance FROM users WHERE age > 60 LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('UNION ALL', $sUs, $lUs);
}

// --- CASE expression ---
{
    $sSt = $sdb->prepare("SELECT name, CASE WHEN balance > 75000 THEN 'high' WHEN balance > 25000 THEN 'medium' ELSE 'low' END as level FROM users LIMIT 100");
    $lSt = $ldb->prepare("SELECT name, CASE WHEN balance > 75000 THEN 'high' WHEN balance > 25000 THEN 'medium' ELSE 'low' END as level FROM users LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('CASE expression', $sUs, $lUs);
}

// --- Complex JOIN + GROUP + HAVING ---
{
    $sql = "SELECT u.age, COUNT(o.id) as cnt, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.age HAVING COUNT(o.id) > 100";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    printRow('Complex JOIN+GRP+HAVING', $sUs, $lUs);
}

// --- Batch INSERT ---
{
    $sInsertSql = 'INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)';
    $lSt = $ldb->prepare('INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)');
    $batchBase = ROW_COUNT * 3 + 100000;
    $batchSize = 100;

    $t = nowUs();
    for ($b = 0; $b < ITERATIONS_HEAVY; $b++) {
        $batch = [];
        for ($j = 0; $j < $batchSize; $j++) {
            $id = $batchBase + $b * $batchSize + $j;
            $batch[] = [$id, ($id % ROW_COUNT) + 1, 99.99, 'pending', '2024-02-01'];
        }
        $sdb->executeBatch($sInsertSql, $batch);
    }
    $sUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    $t = nowUs();
    for ($b = 0; $b < ITERATIONS_HEAVY; $b++) {
        $ldb->exec('BEGIN');
        for ($j = 0; $j < $batchSize; $j++) {
            $id = $batchBase + ITERATIONS_HEAVY * $batchSize + $b * $batchSize + $j;
            $lSt->bindValue(1, $id, SQLITE3_INTEGER);
            $lSt->bindValue(2, ($id % ROW_COUNT) + 1, SQLITE3_INTEGER);
            $lSt->bindValue(3, 99.99, SQLITE3_FLOAT);
            $lSt->bindValue(4, 'pending', SQLITE3_TEXT);
            $lSt->bindValue(5, '2024-02-01', SQLITE3_TEXT);
            $lSt->execute();
        }
        $ldb->exec('COMMIT');
    }
    $lUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    printRow('Batch INSERT (100/tx)', $sUs, $lUs);
}

// ============================================================
// BOTTLENECK HUNTERS
// ============================================================
printHeader('BOTTLENECK HUNTERS');

// --- DISTINCT ---
{
    $sSt = $sdb->prepare('SELECT DISTINCT age FROM users');
    $lSt = $ldb->prepare('SELECT DISTINCT age FROM users');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('DISTINCT', $sUs, $lUs);
}

// --- COUNT DISTINCT ---
{
    $sSt = $sdb->prepare('SELECT COUNT(DISTINCT age) FROM users');
    $lSt = $ldb->prepare('SELECT COUNT(DISTINCT age) FROM users');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $res->fetchArray(SQLITE3_ASSOC);
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('COUNT DISTINCT', $sUs, $lUs);
}

// --- LIKE prefix ---
{
    $sSt = $sdb->prepare("SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100");
    $lSt = $ldb->prepare("SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('LIKE prefix', $sUs, $lUs);
}

// --- LIKE contains ---
{
    $sSt = $sdb->prepare("SELECT * FROM users WHERE email LIKE '%500%' LIMIT 100");
    $lSt = $ldb->prepare("SELECT * FROM users WHERE email LIKE '%500%' LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('LIKE contains', $sUs, $lUs);
}

// --- OR conditions ---
{
    $sSt = $sdb->prepare('SELECT * FROM users WHERE age = 25 OR age = 30 OR age = 35 OR age = 40 LIMIT 100');
    $lSt = $ldb->prepare('SELECT * FROM users WHERE age = 25 OR age = 30 OR age = 35 OR age = 40 LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('OR conditions', $sUs, $lUs);
}

// --- IN list ---
{
    $sSt = $sdb->prepare('SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50, 55, 60, 65) LIMIT 100');
    $lSt = $ldb->prepare('SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50, 55, 60, 65) LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('IN list', $sUs, $lUs);
}

// --- NOT IN subquery ---
{
    $sSt = $sdb->prepare("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100");
    $lSt = $ldb->prepare("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    printRow('NOT IN subquery', $sUs, $lUs);
}

// --- NOT EXISTS subquery ---
{
    $sSt = $sdb->prepare("SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100");
    $lSt = $ldb->prepare("SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    printRow('NOT EXISTS subquery', $sUs, $lUs);
}

// --- OFFSET pagination ---
{
    $sSt = $sdb->prepare('SELECT * FROM users ORDER BY id LIMIT 100 OFFSET $1');
    $lSt = $ldb->prepare('SELECT * FROM users ORDER BY id LIMIT 100 OFFSET ?');
    $offsets = [];
    for ($i = 0; $i < ITERATIONS; $i++) {
        $offsets[] = ($i * 100) % ROW_COUNT;
    }

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query([$offsets[$i]]);
        $lSt->bindValue(1, $offsets[$i], SQLITE3_INTEGER);
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query([$offsets[$i]]);
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $lSt->bindValue(1, $offsets[$i], SQLITE3_INTEGER);
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('OFFSET pagination', $sUs, $lUs);
}

// --- Multi-col ORDER BY ---
{
    $sSt = $sdb->prepare('SELECT * FROM users ORDER BY age DESC, balance ASC LIMIT 100');
    $lSt = $ldb->prepare('SELECT * FROM users ORDER BY age DESC, balance ASC LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Multi-col ORDER BY', $sUs, $lUs);
}

// --- Self JOIN ---
{
    $sql = 'SELECT a.name, b.name as friend FROM users a INNER JOIN users b ON a.age = b.age AND a.id <> b.id LIMIT 100';
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    printRow('Self JOIN', $sUs, $lUs);
}

// --- Multi window funcs ---
{
    $sql = 'SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100';
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Multi window funcs', $sUs, $lUs);
}

// --- Nested subquery ---
{
    $sql = "SELECT * FROM users WHERE age IN (SELECT age FROM users GROUP BY age HAVING COUNT(*) > (SELECT COUNT(*) / 62 FROM users)) LIMIT 100";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Nested subquery', $sUs, $lUs);
}

// --- Multi aggregates ---
{
    $sSt = $sdb->prepare('SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance) FROM users');
    $lSt = $ldb->prepare('SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance) FROM users');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $res->fetchArray(SQLITE3_ASSOC);
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Multi aggregates', $sUs, $lUs);
}

// --- COALESCE ---
{
    $sSt = $sdb->prepare('SELECT COALESCE(name, email) as display_name FROM users LIMIT 100');
    $lSt = $ldb->prepare('SELECT COALESCE(name, email) as display_name FROM users LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('COALESCE', $sUs, $lUs);
}

// --- Expr in WHERE ---
{
    $sSt = $sdb->prepare('SELECT * FROM users WHERE balance / 1000 > 50 LIMIT 100');
    $lSt = $ldb->prepare('SELECT * FROM users WHERE balance / 1000 > 50 LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Expr in WHERE', $sUs, $lUs);
}

// --- Math expressions ---
{
    $sSt = $sdb->prepare('SELECT id, balance * 1.1 + 100 as adjusted FROM users LIMIT 100');
    $lSt = $ldb->prepare('SELECT id, balance * 1.1 + 100 as adjusted FROM users LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Math expressions', $sUs, $lUs);
}

// --- String concat ---
{
    $sSt = $sdb->prepare("SELECT name || ' <' || email || '>' as display FROM users LIMIT 100");
    $lSt = $ldb->prepare("SELECT name || ' <' || email || '>' as display FROM users LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('String concat', $sUs, $lUs);
}

// --- Large result ---
{
    $sSt = $sdb->prepare('SELECT * FROM users ORDER BY id LIMIT 5000');
    $lSt = $ldb->prepare('SELECT * FROM users ORDER BY id LIMIT 5000');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_HEAVY; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_HEAVY;

    printRow('Large result (5000)', $sUs, $lUs);
}

// --- Multiple CTEs ---
{
    $sSt = $sdb->prepare("WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 50000) SELECT y.name, r.balance FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 100");
    $lSt = $ldb->prepare("WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 50000) SELECT y.name, r.balance FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 100");

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Multiple CTEs', $sUs, $lUs);
}

// --- Correlated in SELECT ---
{
    $sql = "SELECT name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Correlated in SELECT', $sUs, $lUs);
}

// --- BETWEEN ---
{
    $sSt = $sdb->prepare('SELECT * FROM users WHERE balance BETWEEN 20000 AND 40000 LIMIT 100');
    $lSt = $ldb->prepare('SELECT * FROM users WHERE balance BETWEEN 20000 AND 40000 LIMIT 100');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('BETWEEN', $sUs, $lUs);
}

// --- GROUP BY 2 cols ---
{
    $sSt = $sdb->prepare('SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active');
    $lSt = $ldb->prepare('SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS_MEDIUM; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS_MEDIUM;

    printRow('GROUP BY 2 cols', $sUs, $lUs);
}

// --- CROSS JOIN small ---
{
    $sSt = $sdb->prepare('SELECT a.age, b.age FROM (SELECT DISTINCT age FROM users LIMIT 10) a CROSS JOIN (SELECT DISTINCT age FROM users LIMIT 10) b');
    $lSt = $ldb->prepare('SELECT a.age, b.age FROM (SELECT DISTINCT age FROM users LIMIT 10) a CROSS JOIN (SELECT DISTINCT age FROM users LIMIT 10) b');

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('CROSS JOIN small', $sUs, $lUs);
}

// --- Derived table (FROM sub) ---
{
    $sql = 'SELECT d.age, d.cnt FROM (SELECT age, COUNT(*) as cnt FROM users GROUP BY age) d WHERE d.cnt > 100';
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Derived table (FROM sub)', $sUs, $lUs);
}

// --- Window ROWS frame ---
{
    $sql = 'SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100';
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Window ROWS frame', $sUs, $lUs);
}

// --- HAVING complex ---
{
    $sql = "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('HAVING complex', $sUs, $lUs);
}

// --- Compare with subquery ---
{
    $sql = "SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100";
    $sSt = $sdb->prepare($sql);
    $lSt = $ldb->prepare($sql);

    for ($i = 0; $i < WARMUP; $i++) {
        $sSt->query();
        $lSt->execute()->finalize();
    }

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $sSt->query();
    }
    $sUs = (nowUs() - $t) / ITERATIONS;

    $t = nowUs();
    for ($i = 0; $i < ITERATIONS; $i++) {
        $res = $lSt->execute();
        $rows = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
        $res->finalize();
    }
    $lUs = (nowUs() - $t) / ITERATIONS;

    printRow('Compare with subquery', $sUs, $lUs);
}

// ============================================================
// Summary
// ============================================================
echo "\n" . str_repeat('=', 80) . "\n";
echo "SCORE: Stoolap {$stoolapWins} wins  |  SQLite {$sqliteWins} wins\n";
echo "\n";
echo "NOTES:\n";
echo "- Both drivers use synchronous methods -fair comparison\n";
echo "- Both use autocommit (each write = implicit transaction + commit)\n";
echo "- Ratio > 1x = Stoolap faster  |  * = SQLite faster\n";
echo str_repeat('=', 80) . "\n";

$sdb->close();
$ldb->close();
