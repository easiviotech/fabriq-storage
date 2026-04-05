<?php

declare(strict_types=1);

namespace Fabriq\Storage;

use PDO;
use RuntimeException;

/**
 * Factory for creating coroutine-safe MySQL connections backed by PDO.
 *
 * PDO becomes coroutine-safe automatically when SWOOLE_HOOK_ALL or
 * SWOOLE_HOOK_PDO_MYSQL is active (enabled in every console command).
 * MysqlConnection wraps PDO to expose the same API the ORM layer expects.
 *
 * Used by ConnectionPool. Each call produces a new, connected client.
 */
final class MysqlConnectionFactory
{
    /**
     * @param array{
     *     host: string,
     *     port: int,
     *     database: string,
     *     username: string,
     *     password: string,
     *     charset?: string,
     * } $config
     */
    public function __construct(
        private readonly array $config,
    ) {}

    /**
     * Create and connect a new MySQL client.
     *
     * @throws RuntimeException on connection failure
     */
    public function create(): MysqlConnection
    {
        $host     = $this->config['host']     ?? '127.0.0.1';
        $port     = (int) ($this->config['port']     ?? 3306);
        $database = $this->config['database'] ?? '';
        $username = $this->config['username'] ?? 'root';
        $password = $this->config['password'] ?? '';
        $charset  = $this->config['charset']  ?? 'utf8mb4';

        $dsn = "mysql:host={$host};port={$port};dbname={$database};charset={$charset}";

        try {
            $pdo = new PDO($dsn, $username, $password, [
                PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_TIMEOUT            => 5,
            ]);
        } catch (\PDOException $e) {
            throw new RuntimeException("MySQL connection failed: {$e->getMessage()}", 0, $e);
        }

        return new MysqlConnection($pdo);
    }

    /**
     * Health check — sends SELECT 1 to verify the connection is alive.
     */
    public static function healthCheck(MysqlConnection $conn): bool
    {
        try {
            $result = $conn->query('SELECT 1');
            return $result !== false;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Create a ConnectionPool pre-configured for MySQL.
     *
     * @param array<string, mixed> $config  Database config (host, port, database, username, password, charset, pool)
     */
    public static function createPool(array $config): ConnectionPool
    {
        $factory = new self($config);
        $poolConfig = $config['pool'] ?? [];

        return new ConnectionPool(
            factory: fn() => $factory->create(),
            healthCheck: fn(mixed $conn) => self::healthCheck($conn),
            maxSize: (int) ($poolConfig['max_size'] ?? 20),
            borrowTimeout: (float) ($poolConfig['borrow_timeout'] ?? 3.0),
            idleTimeout: (float) ($poolConfig['idle_timeout'] ?? 60.0),
        );
    }
}

