<?php

declare(strict_types=1);

namespace Fabriq\Storage;

use Fabriq\Kernel\Context;
use RuntimeException;

/**
 * Abstract base class for tenant-scoped repositories.
 *
 * Enforces that every query includes tenant_id.
 * Subclasses use the DbManager to borrow/release connections.
 *
 * Fail-fast: if no tenant_id is set on Context, all operations throw.
 */
abstract class TenantAwareRepository
{
    public function __construct(
        protected readonly DbManager $db,
    ) {}

    /**
     * Get the current tenant_id from Context. Throws if not set.
     *
     * @throws RuntimeException if tenant_id is missing
     */
    protected function tenantId(): string
    {
        $tenantId = Context::tenantId();

        if ($tenantId === null || $tenantId === '') {
            throw new RuntimeException(
                static::class . ': tenant_id is required but not set on Context. '
                . 'Ensure TenancyMiddleware has run before accessing tenant-scoped data.'
            );
        }

        return $tenantId;
    }

    /**
     * Execute a tenant-scoped query on the app DB.
     *
     * Borrows a connection, executes the callback, and releases.
     *
     * @template T
     * @param callable(mixed): T $callback  Receives a MySQL connection
     * @return T
     */
    protected function withAppDb(callable $callback): mixed
    {
        $this->tenantId(); // Fail-fast guard

        $conn = $this->db->app();
        try {
            return $callback($conn);
        } finally {
            $this->db->releaseApp($conn);
        }
    }

    /**
     * Execute a query on the platform DB.
     *
     * @template T
     * @param callable(mixed): T $callback  Receives a MySQL connection
     * @return T
     */
    protected function withPlatformDb(callable $callback): mixed
    {
        $conn = $this->db->platform();
        try {
            return $callback($conn);
        } finally {
            $this->db->releasePlatform($conn);
        }
    }

    /**
     * Execute a tenant-scoped transaction on the app DB.
     *
     * @template T
     * @param callable(mixed): T $callback
     * @return T
     */
    protected function appTransaction(callable $callback): mixed
    {
        $this->tenantId(); // Fail-fast guard
        return $this->db->transaction('app', $callback);
    }

    /**
     * Helper: build a parameterized INSERT statement.
     *
     * @param string $table
     * @param array<string, mixed> $data Column => value pairs
     * @return array{sql: string, params: list<mixed>}
     */
    protected function buildInsert(string $table, array $data): array
    {
        $columns = array_keys($data);
        $placeholders = array_fill(0, count($columns), '?');

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES (%s)',
            $table,
            implode(', ', $columns),
            implode(', ', $placeholders),
        );

        return ['sql' => $sql, 'params' => array_values($data)];
    }

    /**
     * Helper: execute a prepared statement on a connection.
     *
     * @param mixed $conn    MySQL connection
     * @param string $sql    SQL with ? placeholders
     * @param list<mixed> $params Bind values
     * @return mixed Query result
     */
    protected function execute(mixed $conn, string $sql, array $params = []): mixed
    {
        $stmt = $conn->prepare($sql);

        if ($stmt === false) {
            throw new RuntimeException("MySQL prepare failed: {$conn->error} (SQL: {$sql})");
        }

        $result = $stmt->execute($params);

        if ($result === false) {
            throw new RuntimeException("MySQL execute failed: {$conn->error} (SQL: {$sql})");
        }

        return $result;
    }
}

