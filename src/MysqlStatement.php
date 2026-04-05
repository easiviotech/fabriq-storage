<?php

declare(strict_types=1);

namespace Fabriq\Storage;

use PDO;
use PDOStatement;

/**
 * Wraps PDOStatement to expose the Swoole\Coroutine\MySQL statement API.
 *
 * execute() returns rows (array) for SELECT-like queries and true for writes,
 * matching the behaviour the ORM layer expects from Swoole\Coroutine\MySQL statements.
 */
final class MysqlStatement
{
    public string $error = '';

    public function __construct(
        private readonly PDOStatement $stmt,
        private readonly MysqlConnection $conn,
    ) {}

    /**
     * Execute the prepared statement.
     *
     * @param  array<mixed> $params  Positional (?) or named (:key) parameter values
     * @return array<array<string, mixed>>|bool  Rows for SELECT, true for writes, false on error
     */
    public function execute(array $params = []): array|bool
    {
        $this->error = '';

        try {
            $this->stmt->execute($params ?: null);

            $this->conn->affected_rows = $this->stmt->rowCount();
            $this->conn->insert_id     = (int) $this->conn->lastInsertId();

            if ($this->stmt->columnCount() > 0) {
                return $this->stmt->fetchAll(PDO::FETCH_ASSOC);
            }

            return true;
        } catch (\PDOException $e) {
            $this->error       = $e->getMessage();
            $this->conn->error = $e->getMessage();
            return false;
        }
    }
}
