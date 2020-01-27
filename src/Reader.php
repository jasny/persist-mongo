<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo;

use Jasny\DB\FieldMap\ConfiguredFieldMap;
use Jasny\DB\FieldMap\FieldMapInterface;
use Jasny\DB\Mongo\Model\BSONToPHP;
use Jasny\DB\Mongo\QueryBuilder\Compose\FilterComposer;
use Jasny\DB\Mongo\QueryBuilder\Finalize\ApplyOptions;
use Jasny\DB\QueryBuilder\FilterQueryBuilder;
use Jasny\DB\QueryBuilder\QueryBuilderInterface;
use Jasny\DB\Reader\ReadInterface;
use Jasny\DB\Result\ResultBuilder;
use Jasny\Immutable;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Fetch data from a MongoDB collection
 */
class Reader implements ReadInterface
{
    use Immutable\With;
    use Traits\CollectionTrait;
    use Traits\ResultTrait;
    use Traits\ReadTrait;

    protected LoggerInterface $logger;

    /**
     * Reader constructor.
     */
    public function __construct(QueryBuilderInterface $queryBuilder, ResultBuilder $resultBuilder)
    {
        $this->queryBuilder = $queryBuilder;
        $this->resultBuilder = $resultBuilder;

        $this->logger = new NullLogger();
    }


    /**
     * Enable (debug) logging.
     *
     * @return static
     */
    public function withLogging(LoggerInterface $logger): self
    {
        return $this->withProperty('logger', $logger);
    }

    /**
     * Log a debug message.
     *
     * @param string       $message
     * @param array<mixed> $context
     */
    protected function debug(string $message, array $context): void
    {
        $this->logger->debug(sprintf($message, $this->getCollection()->getCollectionName()), $context);
    }


    /**
     * Get the query builder used by this service.
     */
    public function getQueryBuilder(): QueryBuilderInterface
    {
        return $this->queryBuilder;
    }

    /**
     * Get a copy with a different query builder.
     *
     * @param QueryBuilderInterface $builder
     * @return static
     */
    public function withQueryBuilder(QueryBuilderInterface $builder): self
    {
        return $this->withProperty('queryBuilder', $builder);
    }

    /**
     * Get the result builder used by this service.
     */
    public function getResultBuilder(): ResultBuilder
    {
        return $this->resultBuilder;
    }

    /**
     * Get a copy with a different result builder.
     *
     * @param ResultBuilder $builder
     * @return static
     */
    public function withResultBuilder(ResultBuilder $builder): self
    {
        return $this->withProperty('resultBuilder', $builder);
    }


    /**
     * Create a reader with the standard query and result builder.
     */
    public static function basic(?FieldMapInterface $map = null): self
    {
        $map ??= new ConfiguredFieldMap([]); // NOP

        $queryBuilder = (new FilterQueryBuilder(new FilterComposer()))
            ->withPreparation([$map, 'applyToFilter'])
            ->withFinalization(new ApplyOptions($map));

        return new static(
            $queryBuilder,
            (new ResultBuilder($map))->map(new BSONToPHP()),
        );
    }
}
