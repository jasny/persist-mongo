<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo;

use Jasny\Immutable;
use Jasny\DB\FieldMap\ConfiguredFieldMap;
use Jasny\DB\FieldMap\FieldMapInterface;
use Jasny\DB\Mongo\QueryBuilder\Compose\FilterComposer;
use Jasny\DB\Mongo\QueryBuilder\Compose\SaveComposer;
use Jasny\DB\Mongo\QueryBuilder\Compose\UpdateComposer;
use Jasny\DB\Mongo\QueryBuilder\Finalize\ConflictResolution;
use Jasny\DB\Mongo\QueryBuilder\Finalize\OneOrMany;
use Jasny\DB\QueryBuilder\FilterQueryBuilder;
use Jasny\DB\QueryBuilder\QueryBuilderInterface;
use Jasny\DB\QueryBuilder\SaveQueryBuilder;
use Jasny\DB\QueryBuilder\UpdateQueryBuilder;
use Jasny\DB\Result\ResultBuilder;
use Jasny\DB\Writer\WriteInterface;
use Psr\Log\LoggerInterface;

/**
 * Fetch data from a MongoDB collection
 */
class Writer implements WriteInterface
{
    use Immutable\With;
    use Traits\CollectionTrait;
    use Traits\ResultTrait;
    use Traits\SaveTrait;
    use Traits\UpdateTrait;
    use Traits\DeleteTrait;

    protected LoggerInterface $logger;

    /**
     * Reader constructor.
     */
    public function __construct(
        QueryBuilderInterface $queryBuilder,
        QueryBuilderInterface $updateQueryBuilder,
        QueryBuilderInterface $saveQueryBuilder,
        ResultBuilder $resultBuilder
    ) {
        $this->queryBuilder = $queryBuilder;
        $this->updateQueryBuilder = $updateQueryBuilder;
        $this->saveQueryBuilder = $saveQueryBuilder;
        $this->resultBuilder = $resultBuilder;
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
     * This builder is used for filtering delete and update queries.
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
     * Get the update query builder used by this service.
     */
    public function getUpdateQueryBuilder(): QueryBuilderInterface
    {
        return $this->updateQueryBuilder;
    }

    /**
     * Get a copy with a different query builder.
     *
     * @param QueryBuilderInterface $builder
     * @return static
     */
    public function withUpdateQueryBuilder(QueryBuilderInterface $builder): self
    {
        return $this->withProperty('updateQueryBuilder', $builder);
    }

    /**
     * Get the save query builder used by this service.
     */
    public function getSaveQueryBuilder(): QueryBuilderInterface
    {
        return $this->saveQueryBuilder;
    }

    /**
     * Get a copy with a different save query builder.
     *
     * @param QueryBuilderInterface $builder
     * @return static
     */
    public function withSaveQueryBuilder(QueryBuilderInterface $builder): self
    {
        return $this->withProperty('saveQueryBuilder', $builder);
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
    public function withResultQueryBuilder(ResultBuilder $builder): self
    {
        return $this->withProperty('resultBuilder', $builder);
    }


    /**
     * Create a writer with standard query and result builder.
     */
    public static function basic(?FieldMapInterface $map = null): self
    {
        $map ??= new ConfiguredFieldMap([]); // NULL object

        $filterQueryBuilder = (new FilterQueryBuilder(new FilterComposer()))
            ->withPreparation([$map, 'applyToFilter'])
            ->withFinalization(new OneOrMany());

        $updateQueryBuilder = (new UpdateQueryBuilder(new UpdateComposer()))
            ->withPreparation([$map, 'applyToUpdate']);

        $saveQueryBuilder = (new SaveQueryBuilder(new SaveComposer()))
            ->withPreparation([$map, 'applyToItems'])
            ->withFinalization(new ConflictResolution());

        return new static(
            $filterQueryBuilder,
            $updateQueryBuilder,
            $saveQueryBuilder,
            new ResultBuilder($map)
        );
    }
}
