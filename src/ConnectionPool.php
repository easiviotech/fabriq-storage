<?php

declare(strict_types=1);

namespace SwooleFabric\Storage;

use Swoole\Coroutine\Channel;
use RuntimeException;

/**
 * Generic Channel-based bounded connection pool.
 *
 * Designed for Swoole coroutine clients (MySQL, Redis).
 * Initialized per-worker in onWorkerStart — never shared across workers.
 *
 * Features:
 *  - Bounded size via Swoole\Coroutine\Channel
 *  - Borrow with timeout
 *  - Idle timeout eviction
 *  - Health-check ping on borrow
 *  - Auto-reconnect on broken connections
 */
final class ConnectionPool
{
    private Channel $channel;
    private int $currentSize = 0;
    private bool $closed = false;

    /** @var array<int, float> Connection ID → last-used timestamp */
    private array $lastUsed = [];

    /**
     * @param callable(): mixed         $factory       Creates a new connection
     * @param callable(mixed): bool     $healthCheck   Returns true if connection is alive
     * @param int                       $maxSize       Maximum pool size
     * @param float                     $borrowTimeout Seconds to wait for available connection
     * @param float                     $idleTimeout   Seconds before idle connection is evicted
     */
    public function __construct(
        private readonly mixed $factory,
        private readonly mixed $healthCheck,
        private readonly int $maxSize = 20,
        private readonly float $borrowTimeout = 3.0,
        private readonly float $idleTimeout = 60.0,
    ) {
        $this->channel = new Channel($maxSize);
    }

    /**
     * Borrow a connection from the pool.
     *
     * If the pool is empty and under capacity, creates a new connection.
     * If at capacity, waits up to $borrowTimeout for a released connection.
     *
     * @throws RuntimeException if pool is closed or borrow times out
     */
    public function borrow(): mixed
    {
        if ($this->closed) {
            throw new RuntimeException('Connection pool is closed');
        }

        // Try to pop an existing connection (non-blocking)
        $conn = $this->channel->pop(0.001);

        if ($conn === false) {
            // Channel empty — can we create a new connection?
            if ($this->currentSize < $this->maxSize) {
                return $this->createConnection();
            }

            // At capacity — wait for a released connection
            $conn = $this->channel->pop($this->borrowTimeout);
            if ($conn === false) {
                throw new RuntimeException(
                    "Connection pool exhausted (max={$this->maxSize}, timeout={$this->borrowTimeout}s)"
                );
            }
        }

        // Validate the borrowed connection
        $connId = spl_object_id($conn);

        // Check idle timeout
        $lastUsed = $this->lastUsed[$connId] ?? 0;
        if ($lastUsed > 0 && (microtime(true) - $lastUsed) > $this->idleTimeout) {
            // Connection was idle too long — discard and create new
            $this->destroyConnection($conn);
            return $this->createConnection();
        }

        // Health check
        if (!($this->healthCheck)($conn)) {
            // Connection is broken — discard and create new
            $this->destroyConnection($conn);
            return $this->createConnection();
        }

        return $conn;
    }

    /**
     * Return a connection to the pool.
     *
     * If pool is closed, the connection is discarded.
     */
    public function release(mixed $conn): void
    {
        if ($this->closed) {
            $this->destroyConnection($conn);
            return;
        }

        $connId = spl_object_id($conn);
        $this->lastUsed[$connId] = microtime(true);

        // Push back to channel (non-blocking; if full, discard)
        if (!$this->channel->push($conn, 0.001)) {
            $this->destroyConnection($conn);
        }
    }

    /**
     * Close the pool — drains all connections.
     */
    public function close(): void
    {
        $this->closed = true;

        while (!$this->channel->isEmpty()) {
            $conn = $this->channel->pop(0.001);
            if ($conn !== false) {
                $this->destroyConnection($conn);
            }
        }

        $this->channel->close();
    }

    /**
     * Get current pool statistics.
     *
     * @return array{current_size: int, max_size: int, idle: int, closed: bool}
     */
    public function stats(): array
    {
        return [
            'current_size' => $this->currentSize,
            'max_size' => $this->maxSize,
            'idle' => $this->channel->length(),
            'closed' => $this->closed,
        ];
    }

    /**
     * Get number of connections currently in the pool (idle).
     */
    public function idleCount(): int
    {
        return $this->channel->length();
    }

    /**
     * Get total connections created (active + idle).
     */
    public function size(): int
    {
        return $this->currentSize;
    }

    // ── Internals ────────────────────────────────────────────────────

    private function createConnection(): mixed
    {
        $this->currentSize++;

        try {
            $conn = ($this->factory)();
            $connId = spl_object_id($conn);
            $this->lastUsed[$connId] = microtime(true);
            return $conn;
        } catch (\Throwable $e) {
            $this->currentSize--;
            throw new RuntimeException("Failed to create connection: {$e->getMessage()}", 0, $e);
        }
    }

    private function destroyConnection(mixed $conn): void
    {
        $connId = spl_object_id($conn);
        unset($this->lastUsed[$connId]);
        $this->currentSize = max(0, $this->currentSize - 1);

        // Attempt graceful close
        try {
            if (method_exists($conn, 'close')) {
                $conn->close();
            }
        } catch (\Throwable) {
            // Swallow — connection may already be dead
        }
    }
}

