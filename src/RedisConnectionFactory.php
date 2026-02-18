<?php

declare(strict_types=1);

namespace SwooleFabric\Storage;

use Swoole\Coroutine\Redis;
use RuntimeException;

/**
 * Factory for creating Swoole coroutine Redis connections.
 *
 * Used by ConnectionPool. Each call produces a new, connected Redis client.
 */
final class RedisConnectionFactory
{
    /**
     * @param array{
     *     host: string,
     *     port: int,
     *     password?: string,
     *     database?: int,
     * } $config
     */
    public function __construct(
        private readonly array $config,
    ) {}

    /**
     * Create and connect a new Redis coroutine client.
     *
     * @throws RuntimeException on connection failure
     */
    public function create(): Redis
    {
        $client = new Redis();

        $host = $this->config['host'] ?? '127.0.0.1';
        $port = (int) ($this->config['port'] ?? 6379);

        $connected = $client->connect($host, $port, 5.0);

        if (!$connected) {
            throw new RuntimeException("Redis connection failed to {$host}:{$port}");
        }

        // Authenticate if password is set
        $password = $this->config['password'] ?? '';
        if ($password !== '') {
            $authResult = $client->auth($password);
            if (!$authResult) {
                throw new RuntimeException('Redis authentication failed');
            }
        }

        // Select database
        $database = (int) ($this->config['database'] ?? 0);
        if ($database > 0) {
            $client->select($database);
        }

        return $client;
    }

    /**
     * Health check — sends PING to verify the connection is alive.
     */
    public static function healthCheck(Redis $conn): bool
    {
        try {
            $result = $conn->ping();
            return $result === true || $result === '+PONG' || $result === 'PONG';
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * Create a ConnectionPool pre-configured for Redis.
     *
     * @param array<string, mixed> $config  Redis config (host, port, password, database, pool)
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

