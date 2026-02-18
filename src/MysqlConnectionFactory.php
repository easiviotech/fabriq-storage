<?php

declare(strict_types=1);

namespace SwooleFabric\Storage;

use Swoole\Coroutine\MySQL;
use RuntimeException;

/**
 * Factory for creating Swoole coroutine MySQL connections.
 *
 * Used by ConnectionPool. Each call produces a new, connected MySQL client.
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
     * Create and connect a new MySQL coroutine client.
     *
     * @throws RuntimeException on connection failure
     */
    public function create(): MySQL
    {
        $client = new MySQL();

        $connected = $client->connect([
            'host' => $this->config['host'] ?? '127.0.0.1',
            'port' => (int) ($this->config['port'] ?? 3306),
            'user' => $this->config['username'] ?? 'root',
            'password' => $this->config['password'] ?? '',
            'database' => $this->config['database'] ?? '',
            'charset' => $this->config['charset'] ?? 'utf8mb4',
            'timeout' => 5.0,
        ]);

        if (!$connected) {
            throw new RuntimeException(
                "MySQL connection failed: {$client->connect_error} (errno: {$client->connect_errno})"
            );
        }

        return $client;
    }

    /**
     * Health check — sends SELECT 1 to verify the connection is alive.
     */
    public static function healthCheck(MySQL $conn): bool
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

