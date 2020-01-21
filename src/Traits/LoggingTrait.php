<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Traits;

use Jasny\DB\Reader\ReadInterface;
use Jasny\Immutable;
use MongoDB\Collection;
use Psr\Log\LoggerInterface;

/**
 * Logging (debug) for read and write service.
 */
trait LoggingTrait
{
    protected ?LoggerInterface $logger;

    /**
     * @param string $property
     * @param mixed  $value
     * @return self
     */
    abstract protected function withProperty(string $property, $value);

    /**
     * Get the mongodb collection the associated with the service.
     */
    abstract public function getCollection(): Collection;

    /**
     * Enable (debug) logging.
     *
     * @return static
     */
    public function withLogging(LoggerInterface $logger): ReadInterface
    {
        return $this->withProperty('logger', $logger);
    }

    /**
     * Log a debug message.
     */
    protected function debug(string $message, array $context): void
    {
        if (isset($this->logger)) {
            $this->logger->debug(sprintf($message, $this->getCollection()->getCollectionName()), $context);
        }
    }
}
