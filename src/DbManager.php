<?php

declare(strict_types=1);

namespace Fabriq\Storage;

use Fabriq\Kernel\Config;
use RuntimeException;

/**
 * Database connection manager.
 *
 * Manages named connection pools (platform DB, app DB).
 * Must be initialized per-worker in onWorkerStart.
 *
 * Provides:
 *  - platform() → borrow from platform DB pool
 *  - app()      → borrow from app DB pool
 *  - redis()    → borrow from Redis pool
 *  - transaction(pool, fn) → borrow one connection for a transaction block
 */
final class DbManager
{
    /** @var array<string, ConnectionPool> Named MySQL pools */
    private array $mysqlPools = [];

    /** @var ConnectionPool|null Redis pool */
    private ?ConnectionPool $redisPool = null;

    /**
     * Initialize pools from config. Call in onWorkerStart.
     */
    public function boot(Config $config): void
    {
        // Platform DB pool
        $platformConfig = $config->get('database.platform');
        if (is_array($platformConfig)) {
            $this->mysqlPools['platform'] = MysqlConnectionFactory::createPool($platformConfig);
        }

        // App DB pool
        $appConfig = $config->get('database.app');
        if (is_array($appConfig)) {
            $this->mysqlPools['app'] = MysqlConnectionFactory::createPool($appConfig);
        }

        // Redis pool
        $redisConfig = $config->get('redis');
        if (is_array($redisConfig)) {
            $this->redisPool = RedisConnectionFactory::createPool($redisConfig);
        }
    }

    /**
     * Borrow a MySQL connection from the platform DB pool.
     */
    public function platform(): mixed
    {
        return $this->borrowMysql('platform');
    }

    /**
     * Release a MySQL connection back to the platform DB pool.
     */
    public function releasePlatform(mixed $conn): void
    {
        $this->releaseMysql('platform', $conn);
    }

    /**
     * Borrow a MySQL connection from the app DB pool.
     */
    public function app(): mixed
    {
        return $this->borrowMysql('app');
    }

    /**
     * Release a MySQL connection back to the app DB pool.
     */
    public function releaseApp(mixed $conn): void
    {
        $this->releaseMysql('app', $conn);
    }

    /**
     * Borrow a Redis connection.
     */
    public function redis(): mixed
    {
        if ($this->redisPool === null) {
            throw new RuntimeException('Redis pool not initialized');
        }
        return $this->redisPool->borrow();
    }

    /**
     * Release a Redis connection.
     */
    public function releaseRedis(mixed $conn): void
    {
        $this->redisPool?->release($conn);
    }

    /**
     * Execute a callback within a MySQL transaction.
     *
     * Borrows ONE connection for the full duration, commits on success,
     * rolls back on exception, always releases.
     *
     * @template T
     * @param string $poolName 'platform' or 'app'
     * @param callable(mixed): T $callback  Receives the MySQL connection
     * @return T
     */
    public function transaction(string $poolName, callable $callback): mixed
    {
        $conn = $this->borrowMysql($poolName);

        try {
            $conn->begin();
            $result = $callback($conn);
            $conn->commit();
            return $result;
        } catch (\Throwable $e) {
            try {
                $conn->rollback();
            } catch (\Throwable) {
                // Swallow rollback failure — original error is more important
            }
            throw $e;
        } finally {
            $this->releaseMysql($poolName, $conn);
        }
    }

    /**
     * Get a named MySQL pool.
     */
    public function getMysqlPool(string $name): ConnectionPool
    {
        if (!isset($this->mysqlPools[$name])) {
            throw new RuntimeException("MySQL pool [{$name}] not found");
        }
        return $this->mysqlPools[$name];
    }

    /**
     * Get the Redis pool.
     */
    public function getRedisPool(): ConnectionPool
    {
        if ($this->redisPool === null) {
            throw new RuntimeException('Redis pool not initialized');
        }
        return $this->redisPool;
    }

    /**
     * Get pool statistics for monitoring.
     *
     * @return array<string, array{current_size: int, max_size: int, idle: int, closed: bool}>
     */
    public function stats(): array
    {
        $stats = [];

        foreach ($this->mysqlPools as $name => $pool) {
            $stats["mysql_{$name}"] = $pool->stats();
        }

        if ($this->redisPool !== null) {
            $stats['redis'] = $this->redisPool->stats();
        }

        return $stats;
    }

    /**
     * Shut down all pools. Call on server shutdown.
     */
    public function shutdown(): void
    {
        foreach ($this->mysqlPools as $pool) {
            $pool->close();
        }
        $this->redisPool?->close();
    }

    // ── Internals ────────────────────────────────────────────────────

    private function borrowMysql(string $name): mixed
    {
        if (!isset($this->mysqlPools[$name])) {
            throw new RuntimeException("MySQL pool [{$name}] not initialized");
        }
        return $this->mysqlPools[$name]->borrow();
    }

    private function releaseMysql(string $name, mixed $conn): void
    {
        if (isset($this->mysqlPools[$name])) {
            $this->mysqlPools[$name]->release($conn);
        }
    }
}

