<?php

declare(strict_types=1);

namespace Fabriq\Storage;

use PDO;

/**
 * Wraps PDO to expose the Swoole\Coroutine\MySQL connection API.
 *
 * Allows the ORM layer to continue using the familiar query/prepare/begin/
 * commit/rollback API and property-based error/insert_id/affected_rows
 * without modification, while the underlying driver switches to PDO (which
 * is coroutine-safe when SWOOLE_HOOK_ALL / SWOOLE_HOOK_PDO_MYSQL is active).
 */
final class MysqlConnection
{
    public string $error         = '';
    public int    $insert_id     = 0;
    public int    $affected_rows = 0;

    public function __construct(private readonly PDO $pdo) {}

    /**
     * Run a query with no bound parameters.
     *
     * @return array<array<string, mixed>>|bool  Rows for SELECT, true for writes, false on error
     */
    public function query(string $sql): array|bool
    {
        $this->error = '';

        try {
            $stmt = $this->pdo->query($sql);

            $this->affected_rows = $stmt->rowCount();
            $this->insert_id     = (int) $this->pdo->lastInsertId();

            if ($stmt->columnCount() > 0) {
                return $stmt->fetchAll(PDO::FETCH_ASSOC);
            }

            return true;
        } catch (\PDOException $e) {
            $this->error = $e->getMessage();
            return false;
        }
    }

    /**
     * Prepare a parameterised statement.
     *
     * @return MysqlStatement|false
     */
    public function prepare(string $sql): MysqlStatement|false
    {
        $this->error = '';

        try {
            $stmt = $this->pdo->prepare($sql);
            return new MysqlStatement($stmt, $this);
        } catch (\PDOException $e) {
            $this->error = $e->getMessage();
            return false;
        }
    }

    public function begin(): bool
    {
        try {
            return $this->pdo->beginTransaction();
        } catch (\PDOException $e) {
            $this->error = $e->getMessage();
            return false;
        }
    }

    public function commit(): bool
    {
        try {
            return $this->pdo->commit();
        } catch (\PDOException $e) {
            $this->error = $e->getMessage();
            return false;
        }
    }

    public function rollback(): bool
    {
        try {
            return $this->pdo->rollBack();
        } catch (\PDOException $e) {
            $this->error = $e->getMessage();
            return false;
        }
    }

    /**
     * No-op — PDO closes the connection when the object is garbage-collected.
     * Exists so ConnectionPool::destroyConnection() can call close() generically.
     */
    public function close(): void {}

    /**
     * Expose lastInsertId() so MysqlStatement can read it after execute().
     */
    public function lastInsertId(): string|false
    {
        return $this->pdo->lastInsertId();
    }
}
