<?php

declare(strict_types=1);

namespace Stoolap\Tests;

use PHPUnit\Framework\TestCase;
use Stoolap\Database;
use Stoolap\Statement;
use Stoolap\StoolapException;
use Stoolap\Transaction;

class StoolapTest extends TestCase
{
    private Database $db;

    protected function setUp(): void
    {
        $this->db = Database::open('memory://test_' . getmypid() . '_' . mt_rand());
    }

    protected function tearDown(): void
    {
        $this->db->close();
    }

    // ---- Database basics ----

    public function testVersion(): void
    {
        $version = $this->db->version();
        $this->assertNotEmpty($version);
        $this->assertMatchesRegularExpression('/^\d+\.\d+\.\d+/', $version);
    }

    public function testOpenInMemory(): void
    {
        $db = Database::openInMemory();
        $this->assertNotEmpty($db->version());
        $db->close();
    }

    public function testOpenWithDsn(): void
    {
        $db = Database::open('memory://testdb');
        $this->assertNotEmpty($db->version());
        $db->close();
    }

    public function testClone(): void
    {
        $this->db->exec('CREATE TABLE clone_test (id INTEGER PRIMARY KEY, val TEXT)');
        $this->db->execute('INSERT INTO clone_test VALUES ($1, $2)', [1, 'hello']);

        $clone = $this->db->clone();
        $row = $clone->queryOne('SELECT val FROM clone_test WHERE id = 1');
        $this->assertSame('hello', $row['val']);
        $clone->close();
    }

    public function testDoubleCloseIsNoop(): void
    {
        $db = Database::open('memory://');
        $db->close();
        $db->close(); // Should not throw
        $this->assertTrue(true);
    }

    public function testUseAfterCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->close();
        $this->expectException(StoolapException::class);
        $db->exec('SELECT 1');
    }

    // ---- DDL / Exec ----

    public function testCreateTableAndInsert(): void
    {
        $this->db->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
        $affected = $this->db->exec("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)");
        $this->assertSame(2, $affected);
    }

    public function testDropTable(): void
    {
        $this->db->exec('CREATE TABLE temp_t (id INTEGER)');
        $this->db->exec('DROP TABLE temp_t');

        $this->expectException(StoolapException::class);
        $this->db->exec('SELECT * FROM temp_t');
    }

    // ---- Execute with params ----

    public function testExecuteWithParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

        $affected = $this->db->execute(
            'INSERT INTO t VALUES ($1, $2)',
            [1, 'Alice']
        );
        $this->assertSame(1, $affected);

        $row = $this->db->queryOne('SELECT name FROM t WHERE id = 1');
        $this->assertSame('Alice', $row['name']);
    }

    public function testExecuteNamedParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

        $affected = $this->db->execute(
            'INSERT INTO t VALUES (:id, :name)',
            ['id' => 10, 'name' => 'Named']
        );
        $this->assertSame(1, $affected);

        $row = $this->db->queryOne('SELECT name FROM t WHERE id = 10');
        $this->assertSame('Named', $row['name']);
    }

    // ---- Query ----

    public function testQuery(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3)");

        $rows = $this->db->query('SELECT id, name, score FROM t ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]['id']);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(95.5, $rows[0]['score'], 0.001);
        $this->assertSame(2, $rows[1]['id']);
    }

    public function testQueryWithParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

        $rows = $this->db->query('SELECT name FROM t WHERE id > $1 ORDER BY id', [1]);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testQueryNamedParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");

        $rows = $this->db->query('SELECT name FROM t WHERE id = :id', ['id' => 2]);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testQueryOne(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'one'), (2, 'two')");

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame('one', $row['val']);

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 999');
        $this->assertNull($row);
    }

    public function testQueryRaw(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'a'), (2, 'b')");

        $result = $this->db->queryRaw('SELECT id, name FROM t ORDER BY id');
        $this->assertSame(['id', 'name'], $result['columns']);
        $this->assertCount(2, $result['rows']);
        $this->assertSame(1, $result['rows'][0][0]);
        $this->assertSame('a', $result['rows'][0][1]);
    }

    public function testQueryEmptyResult(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER)');
        $rows = $this->db->query('SELECT * FROM t');
        $this->assertSame([], $rows);
    }

    // ---- Data types ----

    public function testNullValues(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, null]);

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertNull($row['val']);
    }

    public function testBooleanValues(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, flag BOOLEAN)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, true]);
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [2, false]);

        $rows = $this->db->query('SELECT id, flag FROM t ORDER BY id');
        $this->assertTrue($rows[0]['flag']);
        $this->assertFalse($rows[1]['flag']);
    }

    public function testIntegerValues(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, big INTEGER)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, PHP_INT_MAX]);
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [2, PHP_INT_MIN]);
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [3, 0]);

        $rows = $this->db->query('SELECT big FROM t ORDER BY id');
        $this->assertSame(PHP_INT_MAX, $rows[0]['big']);
        $this->assertSame(PHP_INT_MIN, $rows[1]['big']);
        $this->assertSame(0, $rows[2]['big']);
    }

    public function testFloatValues(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val FLOAT)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, 3.14159]);
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [2, -0.001]);

        $rows = $this->db->query('SELECT val FROM t ORDER BY id');
        $this->assertEqualsWithDelta(3.14159, $rows[0]['val'], 0.00001);
        $this->assertEqualsWithDelta(-0.001, $rows[1]['val'], 0.00001);
    }

    public function testTimestampValues(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, ts TIMESTAMP)');
        $this->db->exec("INSERT INTO t VALUES (1, '2024-01-15 10:30:00')");

        $row = $this->db->queryOne('SELECT ts FROM t WHERE id = 1');
        $this->assertStringContainsString('2024-01-15', $row['ts']);
    }

    public function testJsonValues(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, data JSON)');
        $this->db->exec("INSERT INTO t VALUES (1, '{\"name\": \"Alice\", \"age\": 30}')");

        $row = $this->db->queryOne('SELECT data FROM t WHERE id = 1');
        $this->assertIsArray($row['data']);
        $this->assertSame('Alice', $row['data']['name']);
        $this->assertSame(30, $row['data']['age']);
    }

    // ---- Prepared statements ----

    public function testPreparedStatementExec(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

        $stmt = $this->db->prepare('INSERT INTO t VALUES ($1, $2)');
        $stmt->execute([1, 'Alice']);
        $stmt->execute([2, 'Bob']);
        $stmt->execute([3, 'Charlie']);
        $stmt->finalize();

        $rows = $this->db->query('SELECT name FROM t ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testPreparedStatementQuery(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

        $stmt = $this->db->prepare('SELECT name FROM t WHERE id = $1');
        $row = $stmt->queryOne([2]);
        $this->assertSame('Bob', $row['name']);

        $row = $stmt->queryOne([3]);
        $this->assertSame('Charlie', $row['name']);

        $row = $stmt->queryOne([999]);
        $this->assertNull($row);

        $stmt->finalize();
    }

    public function testPreparedStatementNamedParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'Alice')");

        $stmt = $this->db->prepare('SELECT name FROM t WHERE id = :id');
        $row = $stmt->queryOne(['id' => 1]);
        $this->assertSame('Alice', $row['name']);
        $stmt->finalize();
    }

    public function testPreparedStatementSql(): void
    {
        $stmt = $this->db->prepare('SELECT 1');
        $this->assertSame('SELECT 1', $stmt->sql());
        $stmt->finalize();
    }

    public function testPreparedStatementReuse(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');

        $stmt = $this->db->prepare('INSERT INTO t VALUES ($1, $2)');
        for ($i = 0; $i < 100; $i++) {
            $stmt->execute([$i, $i * 10]);
        }
        $stmt->finalize();

        $row = $this->db->queryOne('SELECT COUNT(*) as cnt FROM t');
        $this->assertSame(100, $row['cnt']);
    }

    public function testFinalizedStatementThrows(): void
    {
        $stmt = $this->db->prepare('SELECT 1');
        $stmt->finalize();

        $this->expectException(StoolapException::class);
        $stmt->query();
    }

    // ---- Transactions ----

    public function testTransactionCommit(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

        $tx = $this->db->begin();
        $tx->exec("INSERT INTO t VALUES (1, 'in-tx')");
        $tx->commit();

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame('in-tx', $row['val']);
    }

    public function testTransactionRollback(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'original')");

        $tx = $this->db->begin();
        $tx->exec("DELETE FROM t WHERE id = 1");
        $tx->rollback();

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame('original', $row['val']);
    }

    public function testTransactionQueryWithParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");

        $tx = $this->db->begin();
        $row = $tx->queryOne('SELECT name FROM t WHERE id = $1', [2]);
        $this->assertSame('Bob', $row['name']);
        $tx->commit();
    }

    public function testTransactionExecuteWithParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

        $tx = $this->db->begin();
        $tx->execute('INSERT INTO t VALUES ($1, $2)', [1, 'tx-param']);
        $tx->commit();

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame('tx-param', $row['val']);
    }

    public function testTransactionAutoRollbackOnDestruct(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');

        $tx = $this->db->begin();
        $tx->exec('INSERT INTO t VALUES (1)');
        unset($tx); // Should auto-rollback

        $row = $this->db->queryOne('SELECT COUNT(*) as cnt FROM t');
        $this->assertSame(0, $row['cnt']);
    }

    public function testSnapshotIsolation(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 100)');

        $tx = $this->db->beginSnapshot();
        $row = $tx->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame(100, $row['val']);
        $tx->commit();
    }

    public function testCommittedTransactionThrows(): void
    {
        $tx = $this->db->begin();
        $tx->commit();

        $this->expectException(StoolapException::class);
        $tx->exec('SELECT 1');
    }

    // ---- SQL features ----

    public function testAggregations(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');

        $row = $this->db->queryOne('SELECT SUM(val) as total, AVG(val) as avg_val, COUNT(*) as cnt FROM t');
        $this->assertSame(60, $row['total']);
        $this->assertSame(3, $row['cnt']);
    }

    public function testGroupBy(): void
    {
        $this->db->exec('CREATE TABLE t (category TEXT, amount INTEGER)');
        $this->db->exec("INSERT INTO t VALUES ('A', 10), ('B', 20), ('A', 30), ('B', 40)");

        $rows = $this->db->query('SELECT category, SUM(amount) as total FROM t GROUP BY category ORDER BY category');
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['category']);
        $this->assertEquals(40, $rows[0]['total']);
        $this->assertSame('B', $rows[1]['category']);
        $this->assertEquals(60, $rows[1]['total']);
    }

    public function testJoin(): void
    {
        $this->db->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)');
        $this->db->exec("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
        $this->db->exec('INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)');

        $rows = $this->db->query(
            'SELECT u.name AS name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name'
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(300, $rows[0]['total']);
    }

    public function testSubquery(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');

        $rows = $this->db->query('SELECT id FROM t WHERE val > (SELECT AVG(val) FROM t) ORDER BY id');
        $this->assertCount(1, $rows);
        $this->assertSame(3, $rows[0]['id']);
    }

    public function testDistinct(): void
    {
        $this->db->exec('CREATE TABLE t (val TEXT)');
        $this->db->exec("INSERT INTO t VALUES ('a'), ('b'), ('a'), ('c'), ('b')");

        $rows = $this->db->query('SELECT DISTINCT val FROM t ORDER BY val');
        $this->assertCount(3, $rows);
    }

    public function testOrderByLimit(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (3), (1), (4), (1), (5)');

        $rows = $this->db->query('SELECT id FROM t ORDER BY id LIMIT 3');
        $this->assertCount(3, $rows);
        $this->assertSame(1, $rows[0]['id']);
        $this->assertSame(1, $rows[1]['id']);
        $this->assertSame(3, $rows[2]['id']);
    }

    public function testUnion(): void
    {
        $this->db->exec('CREATE TABLE t1 (id INTEGER)');
        $this->db->exec('CREATE TABLE t2 (id INTEGER)');
        $this->db->exec('INSERT INTO t1 VALUES (1), (2)');
        $this->db->exec('INSERT INTO t2 VALUES (2), (3)');

        $rows = $this->db->query('SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id');
        $this->assertCount(3, $rows);
    }

    public function testCTE(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, parent_id INTEGER, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, NULL, 'root'), (2, 1, 'child1'), (3, 1, 'child2')");

        $rows = $this->db->query(
            'WITH children AS (SELECT * FROM t WHERE parent_id = 1) SELECT name FROM children ORDER BY name'
        );
        $this->assertCount(2, $rows);
        $this->assertSame('child1', $rows[0]['name']);
    }

    public function testWindowFunction(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');

        $rows = $this->db->query('SELECT id, val, SUM(val) OVER (ORDER BY id) as running FROM t ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame(10, $rows[0]['running']);
        $this->assertSame(30, $rows[1]['running']);
        $this->assertSame(60, $rows[2]['running']);
    }

    public function testCaseExpression(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 10), (2, 50), (3, 90)');

        $rows = $this->db->query(
            "SELECT id, CASE WHEN val < 30 THEN 'low' WHEN val < 70 THEN 'mid' ELSE 'high' END as level FROM t ORDER BY id"
        );
        $this->assertSame('low', $rows[0]['level']);
        $this->assertSame('mid', $rows[1]['level']);
        $this->assertSame('high', $rows[2]['level']);
    }

    // ---- Error handling ----

    public function testInvalidSqlThrows(): void
    {
        $this->expectException(StoolapException::class);
        $this->db->exec('INVALID SQL SYNTAX HERE');
    }

    public function testQueryNonexistentTable(): void
    {
        $this->expectException(StoolapException::class);
        $this->db->query('SELECT * FROM nonexistent_table');
    }

    // ---- Trailing semicolons ----

    public function testTrailingSemicolonStripped(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER);');
        $this->db->exec('INSERT INTO t VALUES (1);');
        $rows = $this->db->query('SELECT * FROM t;');
        $this->assertCount(1, $rows);
    }

    // ---- Update / Delete changes count ----

    public function testUpdateReturnsChanges(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');

        $affected = $this->db->exec('UPDATE t SET val = 99 WHERE id <= 2');
        $this->assertSame(2, $affected);
    }

    public function testDeleteReturnsChanges(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
        $this->db->exec('INSERT INTO t VALUES (1), (2), (3)');

        $affected = $this->db->exec('DELETE FROM t WHERE id > 1');
        $this->assertSame(2, $affected);
    }

    // ---- Error handling (constraint violations) ----

    public function testNotNullViolation(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');

        $this->expectException(StoolapException::class);
        $this->db->exec('INSERT INTO t VALUES (1, NULL)');
    }

    public function testDuplicatePrimaryKey(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
        $this->db->exec('INSERT INTO t VALUES (1)');

        $this->expectException(StoolapException::class);
        $this->db->exec('INSERT INTO t VALUES (1)');
    }

    // ---- Edge case params ----

    public function testEmptyStringParam(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, '']);

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame('', $row['val']);
    }

    public function testUnicodeTextParam(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, 'Héllo Wörld 日本語 🌍']);

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame('Héllo Wörld 日本語 🌍', $row['val']);
    }

    public function testVeryLongText(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $long = str_repeat('x', 100_000);
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, $long]);

        $row = $this->db->queryOne('SELECT val FROM t WHERE id = 1');
        $this->assertSame(100_000, strlen($row['val']));
    }

    public function testNegativeNumbers(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, i INTEGER, f FLOAT)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2, $3)', [1, -42, -3.14]);

        $row = $this->db->queryOne('SELECT i, f FROM t WHERE id = 1');
        $this->assertSame(-42, $row['i']);
        $this->assertEqualsWithDelta(-3.14, $row['f'], 0.001);
    }

    public function testZeroValues(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, i INTEGER, f FLOAT)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2, $3)', [1, 0, 0.0]);

        $row = $this->db->queryOne('SELECT i, f FROM t WHERE id = 1');
        $this->assertSame(0, $row['i']);
        $this->assertEqualsWithDelta(0.0, $row['f'], 0.001);
    }

    public function testObjectAsJsonParam(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, data JSON)');
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, ['name' => 'Alice', 'scores' => [1, 2, 3]]]);

        $row = $this->db->queryOne('SELECT data FROM t WHERE id = 1');
        $this->assertSame('Alice', $row['data']['name']);
        $this->assertSame([1, 2, 3], $row['data']['scores']);
    }

    public function testDateTimeAsTimestampParam(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, ts TIMESTAMP)');
        $dt = new \DateTimeImmutable('2024-06-15 12:30:45', new \DateTimeZone('UTC'));
        $this->db->execute('INSERT INTO t VALUES ($1, $2)', [1, $dt]);

        $row = $this->db->queryOne('SELECT ts FROM t WHERE id = 1');
        $this->assertStringContainsString('2024-06-15', $row['ts']);
        $this->assertStringContainsString('12:30:45', $row['ts']);
    }

    public function testAllNullRow(): void
    {
        $this->db->exec('CREATE TABLE t (a TEXT, b INTEGER, c FLOAT)');
        $this->db->exec('INSERT INTO t VALUES (NULL, NULL, NULL)');

        $row = $this->db->queryOne('SELECT a, b, c FROM t');
        $this->assertNull($row['a']);
        $this->assertNull($row['b']);
        $this->assertNull($row['c']);
    }

    public function testMultipleColumnsSameType(): void
    {
        $this->db->exec('CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 2, 3)');

        $row = $this->db->queryOne('SELECT a, b, c FROM t');
        $this->assertSame(1, $row['a']);
        $this->assertSame(2, $row['b']);
        $this->assertSame(3, $row['c']);
    }

    // ---- Large result set ----

    public function testLargeResultSet(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
        $stmt = $this->db->prepare('INSERT INTO t VALUES ($1, $2)');
        for ($i = 0; $i < 1000; $i++) {
            $stmt->execute([$i, $i * 10]);
        }
        $stmt->finalize();

        $rows = $this->db->query('SELECT * FROM t ORDER BY id');
        $this->assertCount(1000, $rows);
        $this->assertSame(0, $rows[0]['id']);
        $this->assertSame(999, $rows[999]['id']);
        $this->assertSame(9990, $rows[999]['val']);
    }

    public function testLargeResultSetRaw(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER)');
        $stmt = $this->db->prepare('INSERT INTO t VALUES ($1)');
        for ($i = 0; $i < 500; $i++) {
            $stmt->execute([$i]);
        }
        $stmt->finalize();

        $result = $this->db->queryRaw('SELECT * FROM t');
        $this->assertCount(500, $result['rows']);
    }

    // ---- Named params edge cases ----

    public function testNamedParamInsideStringLiteralIsNotRewritten(): void
    {
        // Named params inside SQL string literals must NOT be rewritten.
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $this->db->execute(
            'INSERT INTO t VALUES (:id, :val)',
            ['id' => 1, 'val' => 'hello']
        );

        // The ':literal' inside quotes should be left alone, not treated as a param
        $row = $this->db->queryOne(
            "SELECT id, 'prefix:literal' AS tag FROM t WHERE id = :id",
            ['id' => 1]
        );
        $this->assertSame(1, $row['id']);
        $this->assertSame('prefix:literal', $row['tag']);
    }

    public function testRepeatedNamedParam(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, a INTEGER, b INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 10, 20), (2, 10, 30)');

        $rows = $this->db->query(
            'SELECT id FROM t WHERE a = :val OR b = :val ORDER BY id',
            ['val' => 10]
        );
        $this->assertCount(2, $rows);
    }

    // ---- EXPLAIN ----

    public function testExplain(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

        $rows = $this->db->query('EXPLAIN SELECT * FROM t WHERE id = 1');
        $this->assertNotEmpty($rows);
    }

    // ---- Multiple exec statements ----

    public function testMultipleStatementsInExec(): void
    {
        $this->db->exec('CREATE TABLE s1 (id INTEGER)');
        $this->db->exec('CREATE TABLE s2 (id INTEGER)');
        $this->db->exec('INSERT INTO s1 VALUES (1)');
        $this->db->exec('INSERT INTO s2 VALUES (2)');

        $r1 = $this->db->queryOne('SELECT id FROM s1');
        $r2 = $this->db->queryOne('SELECT id FROM s2');
        $this->assertSame(1, $r1['id']);
        $this->assertSame(2, $r2['id']);
    }

    // ---- Transaction with named params ----

    public function testTransactionNamedParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

        $tx = $this->db->begin();
        $tx->execute('INSERT INTO t VALUES (:id, :name)', ['id' => 1, 'name' => 'TxNamed']);
        $row = $tx->queryOne('SELECT name FROM t WHERE id = :id', ['id' => 1]);
        $this->assertSame('TxNamed', $row['name']);
        $tx->commit();
    }

    // ---- Prepared statement queryRaw ----

    public function testPreparedStatementQueryRaw(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'a'), (2, 'b')");

        $stmt = $this->db->prepare('SELECT id, name FROM t ORDER BY id');
        $result = $stmt->queryRaw();
        $this->assertSame(['id', 'name'], $result['columns']);
        $this->assertCount(2, $result['rows']);
        $stmt->finalize();
    }

    // ---- File-based persistence ----

    /**
     * Recursively remove a directory and all its contents.
     * Handles the deep directory structure of v0.4.0 volumes (volumes/table/files).
     */
    private static function removeDir(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $items = scandir($dir);
        foreach ($items as $item) {
            if ($item === '.' || $item === '..') {
                continue;
            }
            $path = $dir . '/' . $item;
            if (is_dir($path)) {
                self::removeDir($path);
            } else {
                @unlink($path);
            }
        }
        @rmdir($dir);
    }

    public function testFilePersistence(): void
    {
        $tmpDir = sys_get_temp_dir() . '/stoolap_php_test_' . getmypid();
        @mkdir($tmpDir, 0755, true);
        $dsn = 'file://' . $tmpDir . '/testdb';

        try {
            $db = Database::open($dsn);
            $db->exec('CREATE TABLE persist (id INTEGER PRIMARY KEY, val TEXT)');
            $db->exec("INSERT INTO persist VALUES (1, 'hello'), (2, 'world')");
            $db->close();

            $db = Database::open($dsn);
            $rows = $db->query('SELECT val FROM persist ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('hello', $rows[0]['val']);
            $this->assertSame('world', $rows[1]['val']);
            $db->close();
        } finally {
            self::removeDir($tmpDir);
        }
    }

    public function testPersistMultipleTablesAndTypes(): void
    {
        $tmpDir = sys_get_temp_dir() . '/stoolap_php_persist_types_' . getmypid();
        @mkdir($tmpDir, 0755, true);
        $dsn = 'file://' . $tmpDir . '/testdb';

        try {
            $db = Database::open($dsn);
            $db->exec('CREATE TABLE kv (id INTEGER PRIMARY KEY, k TEXT, v TEXT)');
            $db->exec('CREATE TABLE nums (id INTEGER PRIMARY KEY, val FLOAT, active BOOLEAN)');
            $db->execute('INSERT INTO kv VALUES ($1, $2, $3)', [1, 'greeting', 'hello']);
            $db->execute('INSERT INTO kv VALUES ($1, $2, $3)', [2, 'farewell', 'goodbye']);
            $db->execute('INSERT INTO nums VALUES ($1, $2, $3)', [1, 3.14, true]);
            $db->execute('INSERT INTO nums VALUES ($1, $2, $3)', [2, 2.71, false]);
            $db->close();

            // Reopen and verify
            $db = Database::open($dsn);
            $kv = $db->query('SELECT * FROM kv ORDER BY k');
            $this->assertCount(2, $kv);
            $this->assertSame('farewell', $kv[0]['k']);
            $this->assertSame('goodbye', $kv[0]['v']);
            $this->assertSame('greeting', $kv[1]['k']);
            $this->assertSame('hello', $kv[1]['v']);

            $nums = $db->query('SELECT * FROM nums ORDER BY id');
            $this->assertCount(2, $nums);
            $this->assertEqualsWithDelta(3.14, $nums[0]['val'], 0.01);
            $this->assertTrue($nums[0]['active']);
            $this->assertFalse($nums[1]['active']);
            $db->close();
        } finally {
            self::removeDir($tmpDir);
        }
    }

    public function testPersistAfterBatchInsert(): void
    {
        $tmpDir = sys_get_temp_dir() . '/stoolap_php_batch_persist_' . getmypid();
        @mkdir($tmpDir, 0755, true);
        $dsn = 'file://' . $tmpDir . '/testdb';

        try {
            $db = Database::open($dsn);
            $db->exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)');
            $db->executeBatch('INSERT INTO items VALUES ($1, $2)', [
                [1, 'alpha'],
                [2, 'beta'],
                [3, 'gamma'],
            ]);
            $db->close();

            $db = Database::open($dsn);
            $rows = $db->query('SELECT * FROM items ORDER BY id');
            $this->assertCount(3, $rows);
            $this->assertSame('alpha', $rows[0]['name']);
            $this->assertSame('gamma', $rows[2]['name']);
            $db->close();
        } finally {
            self::removeDir($tmpDir);
        }
    }

    public function testVolumesDirectoryCreatedOnCheckpoint(): void
    {
        $tmpDir = sys_get_temp_dir() . '/stoolap_php_vol_' . getmypid();
        @mkdir($tmpDir, 0755, true);
        $dbPath = $tmpDir . '/testdb';
        $dsn = 'file://' . $dbPath;

        try {
            $db = Database::open($dsn);
            $db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
            $db->execute('INSERT INTO t VALUES ($1, $2)', [1, 'test']);
            $db->exec('PRAGMA CHECKPOINT');
            $db->close();

            // v0.4.0: volumes/ should exist, snapshots/ should not
            $this->assertDirectoryExists($dbPath . '/volumes');
            $this->assertDirectoryDoesNotExist($dbPath . '/snapshots');

            // Verify data survived
            $db = Database::open($dsn);
            $row = $db->queryOne('SELECT * FROM t WHERE id = $1', [1]);
            $this->assertSame('test', $row['val']);
            $db->close();
        } finally {
            self::removeDir($tmpDir);
        }
    }

    public function testPersistTransactionCommitButNotRollback(): void
    {
        $tmpDir = sys_get_temp_dir() . '/stoolap_php_tx_persist_' . getmypid();
        @mkdir($tmpDir, 0755, true);
        $dsn = 'file://' . $tmpDir . '/testdb';

        try {
            $db = Database::open($dsn);
            $db->exec('CREATE TABLE txp (id INTEGER PRIMARY KEY, val TEXT)');

            // Committed transaction
            $tx = $db->begin();
            $tx->execute('INSERT INTO txp VALUES ($1, $2)', [1, 'kept']);
            $tx->commit();

            // Rolled back transaction
            $tx = $db->begin();
            $tx->execute('INSERT INTO txp VALUES ($1, $2)', [2, 'discarded']);
            $tx->rollback();

            $db->close();

            // Verify only committed data persisted
            $db = Database::open($dsn);
            $rows = $db->query('SELECT * FROM txp ORDER BY id');
            $this->assertCount(1, $rows);
            $this->assertSame('kept', $rows[0]['val']);
            $db->close();
        } finally {
            self::removeDir($tmpDir);
        }
    }

    // ---- Vector support ----

    public function testVectorCreateAndInsert(): void
    {
        $this->db->exec('CREATE TABLE vecs (id INTEGER PRIMARY KEY, embedding VECTOR(3))');
        $this->db->exec("INSERT INTO vecs VALUES (1, '[1.0, 2.0, 3.0]')");
        $this->db->exec("INSERT INTO vecs VALUES (2, '[4.0, 5.0, 6.0]')");

        $rows = $this->db->query('SELECT id FROM vecs ORDER BY id');
        $this->assertCount(2, $rows);
    }

    public function testVectorL2Distance(): void
    {
        $this->db->exec('CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(3))');
        $this->db->exec("INSERT INTO vecs VALUES (1, '[1.0, 0.0, 0.0]')");
        $this->db->exec("INSERT INTO vecs VALUES (2, '[0.0, 1.0, 0.0]')");
        $this->db->exec("INSERT INTO vecs VALUES (3, '[1.0, 1.0, 0.0]')");

        $rows = $this->db->query("SELECT id, VEC_DISTANCE_L2(v, '[1.0, 0.0, 0.0]') AS dist FROM vecs ORDER BY dist LIMIT 2");
        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]['id']);
    }

    public function testVectorCosineDistance(): void
    {
        $this->db->exec('CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(3))');
        $this->db->exec("INSERT INTO vecs VALUES (1, '[1.0, 0.0, 0.0]')");
        $this->db->exec("INSERT INTO vecs VALUES (2, '[0.0, 1.0, 0.0]')");

        $rows = $this->db->query("SELECT id, VEC_DISTANCE_COSINE(v, '[1.0, 0.0, 0.0]') AS dist FROM vecs ORDER BY dist");
        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]['id']);
    }

    public function testVectorDims(): void
    {
        $this->db->exec('CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(4))');
        $this->db->exec("INSERT INTO vecs VALUES (1, '[1.0, 2.0, 3.0, 4.0]')");

        $row = $this->db->queryOne('SELECT VEC_DIMS(v) AS dims FROM vecs WHERE id = 1');
        $this->assertSame(4, $row['dims']);
    }

    // ---- Open DSN variants ----

    public function testOpenWithColonMemory(): void
    {
        $db = Database::open(':memory:');
        $db->exec('CREATE TABLE dsn_colon_test (id INTEGER)');
        $db->exec('INSERT INTO dsn_colon_test VALUES (1)');
        $rows = $db->query('SELECT * FROM dsn_colon_test');
        $this->assertCount(1, $rows);
        $db->close();
    }

    public function testOpenWithEmptyString(): void
    {
        $db = Database::open('');
        $db->exec('CREATE TABLE dsn_empty_test (id INTEGER)');
        $rows = $db->query('SELECT * FROM dsn_empty_test');
        $this->assertSame([], $rows);
        $db->close();
    }

    // ---- Query with no params argument ----

    public function testQueryNoParamsArgument(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'test')");

        $rows = $this->db->query('SELECT * FROM t');
        $this->assertCount(1, $rows);

        $row = $this->db->queryOne('SELECT * FROM t');
        $this->assertSame(1, $row['id']);
    }

    // ---- Batch insert via prepared statement ----

    public function testBatchInsertViaPreparedStatement(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)');

        $stmt = $this->db->prepare('INSERT INTO t VALUES ($1, $2, $3)');
        $data = [
            [1, 'Alice', 95.5],
            [2, 'Bob', 87.3],
            [3, 'Charlie', 92.1],
            [4, 'Diana', 88.7],
            [5, 'Eve', 91.0],
        ];
        foreach ($data as $row) {
            $stmt->execute($row);
        }
        $stmt->finalize();

        $row = $this->db->queryOne('SELECT COUNT(*) AS cnt FROM t');
        $this->assertSame(5, $row['cnt']);
    }

    // ---- Transaction batch insert ----

    public function testTransactionBatchInsert(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');

        $tx = $this->db->begin();
        for ($i = 0; $i < 50; $i++) {
            $tx->execute('INSERT INTO t VALUES ($1, $2)', [$i, "val_$i"]);
        }
        $tx->commit();

        $row = $this->db->queryOne('SELECT COUNT(*) AS cnt FROM t');
        $this->assertSame(50, $row['cnt']);
    }

    // ---- HAVING clause ----

    public function testHaving(): void
    {
        $this->db->exec('CREATE TABLE t (category TEXT, amount INTEGER)');
        $this->db->exec("INSERT INTO t VALUES ('A', 10), ('B', 20), ('A', 30), ('B', 40), ('C', 5)");

        $rows = $this->db->query('SELECT category, SUM(amount) AS total FROM t GROUP BY category HAVING SUM(amount) > 15 ORDER BY category');
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['category']);
        $this->assertSame('B', $rows[1]['category']);
    }

    // ---- LIKE operator ----

    public function testLikeOperator(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, name TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alicia')");

        $rows = $this->db->query("SELECT name FROM t WHERE name LIKE 'Ali%' ORDER BY name");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Alicia', $rows[1]['name']);
    }

    // ---- IN operator ----

    public function testInOperator(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");

        $rows = $this->db->query("SELECT val FROM t WHERE id IN (1, 3) ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame('a', $rows[0]['val']);
        $this->assertSame('c', $rows[1]['val']);
    }

    // ---- BETWEEN operator ----

    public function testBetweenOperator(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)');

        $rows = $this->db->query('SELECT id FROM t WHERE val BETWEEN 15 AND 35 ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(2, $rows[0]['id']);
        $this->assertSame(3, $rows[1]['id']);
    }

    // ---- COALESCE ----

    public function testCoalesce(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, a TEXT, b TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, NULL, 'fallback'), (2, 'primary', 'fallback')");

        $rows = $this->db->query('SELECT id, COALESCE(a, b) AS result FROM t ORDER BY id');
        $this->assertSame('fallback', $rows[0]['result']);
        $this->assertSame('primary', $rows[1]['result']);
    }

    // ---- LEFT JOIN ----

    public function testLeftJoin(): void
    {
        $this->db->exec('CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->exec('CREATE TABLE t2 (id INTEGER, t1_id INTEGER, val TEXT)');
        $this->db->exec("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')");
        $this->db->exec("INSERT INTO t2 VALUES (1, 1, 'x')");

        $rows = $this->db->query(
            'SELECT t1.name AS name, t2.val AS val FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id'
        );
        $this->assertCount(2, $rows);
        $this->assertSame('x', $rows[0]['val']);
        $this->assertNull($rows[1]['val']);
    }

    // ---- UNION ALL ----

    public function testUnionAll(): void
    {
        $this->db->exec('CREATE TABLE t1 (id INTEGER)');
        $this->db->exec('CREATE TABLE t2 (id INTEGER)');
        $this->db->exec('INSERT INTO t1 VALUES (1), (2)');
        $this->db->exec('INSERT INTO t2 VALUES (2), (3)');

        $rows = $this->db->query('SELECT id FROM t1 UNION ALL SELECT id FROM t2 ORDER BY id');
        $this->assertCount(4, $rows);
    }

    // ---- EXISTS subquery ----

    public function testExistsSubquery(): void
    {
        $this->db->exec('CREATE TABLE t1 (id INTEGER)');
        $this->db->exec('CREATE TABLE t2 (t1_id INTEGER)');
        $this->db->exec('INSERT INTO t1 VALUES (1), (2), (3)');
        $this->db->exec('INSERT INTO t2 VALUES (1), (3)');

        $rows = $this->db->query('SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id) ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]['id']);
        $this->assertSame(3, $rows[1]['id']);
    }

    // ---- String functions ----

    public function testStringFunctions(): void
    {
        $row = $this->db->queryOne("SELECT UPPER('hello') AS u, LOWER('WORLD') AS l, LENGTH('test') AS len");
        $this->assertSame('HELLO', $row['u']);
        $this->assertSame('world', $row['l']);
        $this->assertSame(4, $row['len']);
    }

    // ---- Math functions ----

    public function testMathFunctions(): void
    {
        $row = $this->db->queryOne('SELECT ABS(-5) AS a, ROUND(3.7) AS r');
        $this->assertEquals(5, $row['a']);
        $this->assertEquals(4, $row['r']);
    }

    // ---- COUNT DISTINCT ----

    public function testCountDistinct(): void
    {
        $this->db->exec('CREATE TABLE t (val TEXT)');
        $this->db->exec("INSERT INTO t VALUES ('a'), ('b'), ('a'), ('c')");

        $row = $this->db->queryOne('SELECT COUNT(DISTINCT val) AS cnt FROM t');
        $this->assertSame(3, $row['cnt']);
    }

    // ---- OFFSET / LIMIT ----

    public function testOffsetLimit(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1), (2), (3), (4), (5)');

        $rows = $this->db->query('SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2');
        $this->assertCount(2, $rows);
        $this->assertSame(3, $rows[0]['id']);
        $this->assertSame(4, $rows[1]['id']);
    }

    // ---- Error on queryRaw ----

    public function testQueryRawInvalidSqlThrows(): void
    {
        $this->expectException(StoolapException::class);
        $this->db->queryRaw('SELECT * FROM nonexistent');
    }

    // ---- Empty table queryRaw ----

    public function testQueryRawEmptyResult(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER)');
        $result = $this->db->queryRaw('SELECT * FROM t');
        $this->assertSame(['id'], $result['columns']);
        $this->assertSame([], $result['rows']);
    }

    // ---- Prepared statement with no params ----

    public function testPreparedStatementNoParams(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER)');
        $this->db->exec('INSERT INTO t VALUES (1), (2)');

        $stmt = $this->db->prepare('SELECT * FROM t ORDER BY id');
        $rows = $stmt->query();
        $this->assertCount(2, $rows);
        $stmt->finalize();
    }

    // ---- Transaction queryRaw ----

    public function testTransactionQueryRaw(): void
    {
        $this->db->exec('CREATE TABLE t (id INTEGER, val TEXT)');
        $this->db->exec("INSERT INTO t VALUES (1, 'a')");

        $tx = $this->db->begin();
        $result = $tx->queryRaw('SELECT id, val FROM t');
        $this->assertSame(['id', 'val'], $result['columns']);
        $this->assertCount(1, $result['rows']);
        $tx->commit();
    }

    // ---- executeBatch ----

    public function testDatabaseExecuteBatch(): void
    {
        $this->db->exec('CREATE TABLE batch (id INTEGER PRIMARY KEY, name TEXT, val FLOAT)');

        $affected = $this->db->executeBatch(
            'INSERT INTO batch (id, name, val) VALUES ($1, $2, $3)',
            [
                [1, 'alice', 1.5],
                [2, 'bob', 2.5],
                [3, 'charlie', 3.5],
            ]
        );

        $this->assertSame(3, $affected);

        $rows = $this->db->query('SELECT * FROM batch ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('alice', $rows[0]['name']);
        $this->assertSame('charlie', $rows[2]['name']);
    }

    public function testDatabaseExecuteBatchEmpty(): void
    {
        $this->db->exec('CREATE TABLE batch2 (id INTEGER)');
        $affected = $this->db->executeBatch('INSERT INTO batch2 VALUES ($1)', []);
        $this->assertSame(0, $affected);
    }

    public function testDatabaseExecuteBatchRollsBackOnError(): void
    {
        $this->db->exec('CREATE TABLE batch3 (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->execute('INSERT INTO batch3 VALUES ($1, $2)', [1, 'existing']);

        try {
            // Second row has duplicate PK — should fail and rollback all
            $this->db->executeBatch(
                'INSERT INTO batch3 VALUES ($1, $2)',
                [
                    [2, 'new'],
                    [1, 'duplicate'],  // will fail
                ]
            );
            $this->fail('Expected exception');
        } catch (StoolapException $e) {
            // expected
        }

        // Only the original row should exist (batch was rolled back)
        $rows = $this->db->query('SELECT * FROM batch3');
        $this->assertCount(1, $rows);
        $this->assertSame('existing', $rows[0]['name']);
    }

    public function testTransactionExecuteBatch(): void
    {
        $this->db->exec('CREATE TABLE txbatch (id INTEGER PRIMARY KEY, val TEXT)');

        $tx = $this->db->begin();
        $affected = $tx->executeBatch(
            'INSERT INTO txbatch VALUES ($1, $2)',
            [
                [1, 'a'],
                [2, 'b'],
            ]
        );
        $this->assertSame(2, $affected);
        $tx->commit();

        $rows = $this->db->query('SELECT * FROM txbatch ORDER BY id');
        $this->assertCount(2, $rows);
    }

    public function testTransactionExecuteBatchRollback(): void
    {
        $this->db->exec('CREATE TABLE txbatch2 (id INTEGER PRIMARY KEY, val TEXT)');

        $tx = $this->db->begin();
        $tx->executeBatch(
            'INSERT INTO txbatch2 VALUES ($1, $2)',
            [[1, 'a'], [2, 'b']]
        );
        $tx->rollback();

        $rows = $this->db->query('SELECT * FROM txbatch2');
        $this->assertCount(0, $rows);
    }

    // ---- Bug fix: batch param count mismatch ----

    public function testDatabaseExecuteBatchParamCountMismatchThrows(): void
    {
        $this->db->exec('CREATE TABLE bm (id INTEGER PRIMARY KEY, name TEXT)');

        $this->expectException(StoolapException::class);
        $this->db->executeBatch(
            'INSERT INTO bm VALUES ($1, $2)',
            [
                [1, 'alice'],
                [2],          // wrong param count
            ]
        );
    }

    public function testTransactionExecuteBatchParamCountMismatchThrows(): void
    {
        $this->db->exec('CREATE TABLE bm2 (id INTEGER PRIMARY KEY, name TEXT)');

        $tx = $this->db->begin();
        try {
            $tx->executeBatch(
                'INSERT INTO bm2 VALUES ($1, $2)',
                [
                    [1, 'alice'],
                    [2],          // wrong param count
                ]
            );
            $this->fail('Expected exception');
        } catch (StoolapException $e) {
            $tx->rollback();
            $this->assertStringContainsString('parameter count mismatch', $e->getMessage());
        }
    }

    // ---- Bug fix: named params in executeBatch ----

    public function testDatabaseExecuteBatchNamedParams(): void
    {
        $this->db->exec('CREATE TABLE bn (id INTEGER PRIMARY KEY, name TEXT)');

        $affected = $this->db->executeBatch(
            'INSERT INTO bn VALUES (:id, :name)',
            [
                ['id' => 1, 'name' => 'alice'],
                ['id' => 2, 'name' => 'bob'],
                ['id' => 3, 'name' => 'charlie'],
            ]
        );
        $this->assertSame(3, $affected);

        $rows = $this->db->query('SELECT * FROM bn ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('alice', $rows[0]['name']);
        $this->assertSame('bob', $rows[1]['name']);
        $this->assertSame('charlie', $rows[2]['name']);
    }

    public function testTransactionExecuteBatchNamedParams(): void
    {
        $this->db->exec('CREATE TABLE bn2 (id INTEGER PRIMARY KEY, name TEXT)');

        $tx = $this->db->begin();
        $affected = $tx->executeBatch(
            'INSERT INTO bn2 VALUES (:id, :name)',
            [
                ['id' => 1, 'name' => 'alice'],
                ['id' => 2, 'name' => 'bob'],
            ]
        );
        $this->assertSame(2, $affected);
        $tx->commit();

        $rows = $this->db->query('SELECT * FROM bn2 ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('alice', $rows[0]['name']);
        $this->assertSame('bob', $rows[1]['name']);
    }

    // ---- Bug fix: string literal with colon not rewritten in batch SQL ----

    public function testExecuteBatchDoesNotRewriteColonInStringLiteral(): void
    {
        $this->db->exec('CREATE TABLE bl (id INTEGER PRIMARY KEY, tag TEXT)');

        // The ':literal' in the SQL string constant should stay as-is
        $affected = $this->db->executeBatch(
            "INSERT INTO bl VALUES (:id, 'prefix:literal')",
            [
                ['id' => 1],
                ['id' => 2],
            ]
        );
        $this->assertSame(2, $affected);

        $rows = $this->db->query('SELECT * FROM bl ORDER BY id');
        $this->assertSame('prefix:literal', $rows[0]['tag']);
        $this->assertSame('prefix:literal', $rows[1]['tag']);
    }

    // ---- Bug fix: unsupported param types rejected ----

    public function testResourceParamThrowsOnExecute(): void
    {
        $this->db->exec('CREATE TABLE rp (id INTEGER, val TEXT)');
        $fp = fopen('php://memory', 'r');
        try {
            $this->expectException(StoolapException::class);
            $this->expectExceptionMessage('Unsupported parameter type');
            $this->db->execute('INSERT INTO rp VALUES ($1, $2)', [1, $fp]);
        } finally {
            fclose($fp);
        }
    }

    public function testResourceParamThrowsOnPreparedStatement(): void
    {
        $this->db->exec('CREATE TABLE rp2 (id INTEGER, val TEXT)');
        $stmt = $this->db->prepare('INSERT INTO rp2 VALUES ($1, $2)');
        $stmt->execute([1, 'valid']);

        $fp = fopen('php://memory', 'r');
        try {
            $this->expectException(StoolapException::class);
            $this->expectExceptionMessage('Unsupported parameter type');
            $stmt->execute([2, $fp]);
        } finally {
            fclose($fp);
            $stmt->finalize();
        }
    }

    public function testResourceParamThrowsOnBatch(): void
    {
        $this->db->exec('CREATE TABLE rp3 (id INTEGER, val TEXT)');
        $fp = fopen('php://memory', 'r');
        try {
            $this->expectException(StoolapException::class);
            $this->expectExceptionMessage('Unsupported parameter type');
            $this->db->executeBatch('INSERT INTO rp3 VALUES ($1, $2)', [
                [1, 'first'],
                [2, $fp],
            ]);
        } finally {
            fclose($fp);
        }
    }

    // ---- Bug fix: Transaction::executeBatch after DB close ----

    public function testTransactionExecuteBatchAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE txclose (id INTEGER, val TEXT)');
        $tx = $db->begin();
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $tx->executeBatch('INSERT INTO txclose VALUES ($1, $2)', [[1, 'a']]);
    }

    public function testTransactionSurvivesDbUnset(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE txsurv (id INTEGER)');
        $db->execute('INSERT INTO txsurv VALUES ($1)', [1]);

        $tx = $db->begin();
        unset($db);
        gc_collect_cycles();

        // The tx ref keeps the DB alive, so query still works
        $rows = $tx->query('SELECT * FROM txsurv');
        $this->assertCount(1, $rows);
        $tx->commit();
    }

    // ---- Fix: associative array with positional SQL throws ----

    public function testAssocArrayWithPositionalSqlThrowsOnExecute(): void
    {
        $this->db->exec('CREATE TABLE apos (id INTEGER PRIMARY KEY, name TEXT)');
        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Associative array parameters require named placeholders');
        $this->db->execute('INSERT INTO apos VALUES ($1, $2)', ['id' => 1, 'name' => 'Alice']);
    }

    public function testAssocArrayWithPositionalSqlThrowsOnQuery(): void
    {
        $this->db->exec('CREATE TABLE apos2 (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->execute('INSERT INTO apos2 VALUES ($1, $2)', [1, 'Alice']);
        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Associative array parameters require named placeholders');
        $this->db->query('SELECT * FROM apos2 WHERE id = $1', ['id' => 1]);
    }

    public function testAssocArrayWithPositionalSqlThrowsOnPreparedStmt(): void
    {
        $this->db->exec('CREATE TABLE apos3 (id INTEGER PRIMARY KEY, name TEXT)');
        $stmt = $this->db->prepare('INSERT INTO apos3 VALUES ($1, $2)');
        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Associative array parameters require named placeholders');
        $stmt->execute(['id' => 1, 'name' => 'Alice']);
    }

    public function testAssocArrayWithPositionalSqlThrowsOnBatch(): void
    {
        $this->db->exec('CREATE TABLE apos4 (id INTEGER PRIMARY KEY, name TEXT)');
        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Associative array parameters require named placeholders');
        $this->db->executeBatch('INSERT INTO apos4 VALUES ($1, $2)', [
            ['id' => 1, 'name' => 'Alice'],
        ]);
    }

    public function testAssocArrayWithPositionalSqlThrowsOnTxExecute(): void
    {
        $this->db->exec('CREATE TABLE apos5 (id INTEGER PRIMARY KEY, name TEXT)');
        $tx = $this->db->begin();
        try {
            $this->expectException(StoolapException::class);
            $this->expectExceptionMessage('Associative array parameters require named placeholders');
            $tx->execute('INSERT INTO apos5 VALUES ($1, $2)', ['id' => 1, 'name' => 'Alice']);
        } finally {
            if ($tx) $tx->rollback();
        }
    }

    public function testAssocArrayWithPositionalSqlThrowsOnTxBatch(): void
    {
        $this->db->exec('CREATE TABLE apos6 (id INTEGER PRIMARY KEY, name TEXT)');
        $tx = $this->db->begin();
        try {
            $this->expectException(StoolapException::class);
            $this->expectExceptionMessage('Associative array parameters require named placeholders');
            $tx->executeBatch('INSERT INTO apos6 VALUES ($1, $2)', [
                ['id' => 1, 'name' => 'Alice'],
            ]);
        } finally {
            if ($tx) $tx->rollback();
        }
    }

    // ---- Fix: named params in comments are ignored ----

    public function testNamedParamInBlockCommentIgnored(): void
    {
        $this->db->exec('CREATE TABLE cmt (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->execute(
            'INSERT INTO cmt VALUES (:id, :name) /* :ignored */',
            ['id' => 1, 'name' => 'Alice']
        );
        $row = $this->db->queryOne('SELECT * FROM cmt WHERE id = 1');
        $this->assertSame('Alice', $row['name']);
    }

    public function testNamedParamInLineCommentIgnored(): void
    {
        $this->db->exec('CREATE TABLE cmt2 (id INTEGER PRIMARY KEY, name TEXT)');
        $this->db->execute(
            "INSERT INTO cmt2 VALUES (:id, :name) -- :ignored",
            ['id' => 1, 'name' => 'Bob']
        );
        $row = $this->db->queryOne('SELECT * FROM cmt2 WHERE id = 1');
        $this->assertSame('Bob', $row['name']);
    }

    public function testQueryWithCommentContainingColonParam(): void
    {
        $this->db->exec('CREATE TABLE cmt3 (id INTEGER PRIMARY KEY, val TEXT)');
        $this->db->execute('INSERT INTO cmt3 VALUES ($1, $2)', [1, 'test']);
        // :fake inside comment should not be treated as a param
        $rows = $this->db->query(
            'SELECT * FROM cmt3 WHERE id = :id /* filter by :fake_param */',
            ['id' => 1]
        );
        $this->assertCount(1, $rows);
        $this->assertSame('test', $rows[0]['val']);
    }

    // ---- Fix: consistent DB-closed checks on Transaction methods ----

    public function testTransactionExecAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE txc1 (id INTEGER)');
        $tx = $db->begin();
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $tx->exec('INSERT INTO txc1 VALUES (1)');
    }

    public function testTransactionExecuteAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE txc2 (id INTEGER)');
        $tx = $db->begin();
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $tx->execute('INSERT INTO txc2 VALUES ($1)', [1]);
    }

    public function testTransactionQueryAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE txc3 (id INTEGER)');
        $tx = $db->begin();
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $tx->query('SELECT * FROM txc3');
    }

    public function testTransactionCommitAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE txc4 (id INTEGER)');
        $tx = $db->begin();
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $tx->commit();
    }

    public function testTransactionRollbackAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE txc5 (id INTEGER)');
        $tx = $db->begin();
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $tx->rollback();
    }

    // ---- Fix: prepared statements reject use after DB close ----

    public function testPreparedStatementExecuteAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE stc1 (id INTEGER, val TEXT)');
        $stmt = $db->prepare('INSERT INTO stc1 VALUES ($1, $2)');
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $stmt->execute([1, 'test']);
    }

    public function testPreparedStatementQueryAfterDbCloseThrows(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE stc2 (id INTEGER)');
        $db->execute('INSERT INTO stc2 VALUES ($1)', [1]);
        $stmt = $db->prepare('SELECT * FROM stc2 WHERE id = $1');
        $db->close();

        $this->expectException(StoolapException::class);
        $this->expectExceptionMessage('Database has been closed');
        $stmt->query([1]);
    }

    public function testPreparedStatementSurvivesDbUnset(): void
    {
        $db = Database::open('memory://');
        $db->exec('CREATE TABLE stc3 (id INTEGER)');
        $db->execute('INSERT INTO stc3 VALUES ($1)', [1]);
        $stmt = $db->prepare('SELECT * FROM stc3');
        unset($db);
        gc_collect_cycles();

        // stmt ref keeps the DB alive, so query still works
        $rows = $stmt->query();
        $this->assertCount(1, $rows);
        $stmt->finalize();
    }

    // ---- Fix: queryOne auto-injects LIMIT 1 ----

    public function testQueryOneAutoLimit(): void
    {
        $this->db->exec('CREATE TABLE ql (id INTEGER PRIMARY KEY)');
        for ($i = 1; $i <= 100; $i++) {
            $this->db->execute('INSERT INTO ql VALUES ($1)', [$i]);
        }

        // Without explicit LIMIT — LIMIT 1 auto-injected
        $row = $this->db->queryOne('SELECT * FROM ql ORDER BY id');
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
    }

    public function testQueryOneRespectsExistingLimit(): void
    {
        $this->db->exec('CREATE TABLE ql2 (id INTEGER PRIMARY KEY)');
        for ($i = 1; $i <= 10; $i++) {
            $this->db->execute('INSERT INTO ql2 VALUES ($1)', [$i]);
        }

        // Explicit LIMIT should not be overridden
        $row = $this->db->queryOne('SELECT * FROM ql2 ORDER BY id LIMIT 1');
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
    }

    public function testQueryOneAutoLimitInTransaction(): void
    {
        $this->db->exec('CREATE TABLE ql3 (id INTEGER PRIMARY KEY)');
        for ($i = 1; $i <= 50; $i++) {
            $this->db->execute('INSERT INTO ql3 VALUES ($1)', [$i]);
        }

        $tx = $this->db->begin();
        $row = $tx->queryOne('SELECT * FROM ql3 ORDER BY id');
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
        $tx->rollback();
    }

    public function testQueryOneLimitNotInjectedInsideStringLiteral(): void
    {
        $this->db->exec('CREATE TABLE ql4 (id INTEGER PRIMARY KEY, note TEXT)');
        $this->db->execute("INSERT INTO ql4 VALUES (\$1, \$2)", [1, 'no limit here']);
        $this->db->execute("INSERT INTO ql4 VALUES (\$1, \$2)", [2, 'another']);

        // "limit" in a column value, not in SQL — LIMIT 1 should still be injected
        $row = $this->db->queryOne("SELECT * FROM ql4 ORDER BY id");
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
    }

    public function testQueryOneColumnNamedLimitDoesNotSuppressInjection(): void
    {
        $this->db->exec('CREATE TABLE ql5 (id INTEGER PRIMARY KEY, my_limit INTEGER)');
        for ($i = 1; $i <= 10; $i++) {
            $this->db->execute('INSERT INTO ql5 VALUES ($1, $2)', [$i, $i * 100]);
        }

        // Column "my_limit" should NOT suppress LIMIT 1 injection
        $row = $this->db->queryOne('SELECT id, my_limit FROM ql5 ORDER BY id');
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
    }

    public function testQueryOneLimitInSubqueryDoesNotSuppressOuterInjection(): void
    {
        $this->db->exec('CREATE TABLE ql6a (id INTEGER PRIMARY KEY, val TEXT)');
        $this->db->exec('CREATE TABLE ql6b (id INTEGER PRIMARY KEY)');
        for ($i = 1; $i <= 10; $i++) {
            $this->db->execute('INSERT INTO ql6a VALUES ($1, $2)', [$i, "val_$i"]);
            $this->db->execute('INSERT INTO ql6b VALUES ($1)', [$i]);
        }

        // LIMIT inside subquery should NOT suppress outer LIMIT 1 injection
        $row = $this->db->queryOne(
            'SELECT * FROM ql6a WHERE id IN (SELECT id FROM ql6b LIMIT 5) ORDER BY id'
        );
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
    }

    public function testQueryOneAutoLimitWithTrailingBlockComment(): void
    {
        $this->db->exec('CREATE TABLE ql7 (id INTEGER PRIMARY KEY, val TEXT)');
        for ($i = 1; $i <= 5; $i++) {
            $this->db->execute('INSERT INTO ql7 VALUES ($1, $2)', [$i, "v$i"]);
        }

        // LIMIT 1 must be inserted before the trailing block comment
        $row = $this->db->queryOne(
            'SELECT * FROM ql7 ORDER BY id /* trailing block comment */'
        );
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
    }

    public function testQueryOneAutoLimitWithTrailingLineComment(): void
    {
        $this->db->exec('CREATE TABLE ql8 (id INTEGER PRIMARY KEY, val TEXT)');
        for ($i = 1; $i <= 5; $i++) {
            $this->db->execute('INSERT INTO ql8 VALUES ($1, $2)', [$i, "v$i"]);
        }

        // LIMIT 1 must be inserted before the trailing line comment
        $row = $this->db->queryOne(
            "SELECT * FROM ql8 ORDER BY id -- trailing line comment"
        );
        $this->assertNotNull($row);
        $this->assertSame(1, $row['id']);
    }
}
