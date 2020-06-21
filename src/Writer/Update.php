<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Writer;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Exception\UnsupportedFeatureException;
use Jasny\DB\Filter\Prepare\MapFilter;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\AbstractService;
use Jasny\DB\Mongo\Filter\FilterComposer;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Mongo\Query\UpdateQuery;
use Jasny\DB\Mongo\QueryBuilder\Finalize\OneOrMany;
use Jasny\DB\Mongo\Update\UpdateComposer;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\QueryBuilder\FilterQueryBuilder;
use Jasny\DB\QueryBuilder\QueryBuilderInterface;
use Jasny\DB\QueryBuilder\UpdateQueryBuilder;
use Jasny\DB\Result\Result;
use Jasny\DB\Update\Prepare\MapUpdate;
use Jasny\DB\Update\UpdateInstruction;
use Jasny\DB\Writer\WriteInterface;
use Jasny\Immutable;
use MongoDB\BulkWriteResult;
use MongoDB\UpdateResult;

/**
 * Update data of a MongoDB collection.
 */
class Update extends AbstractService implements WriteInterface
{
    use Immutable\With;

    protected QueryBuilderInterface $updateQueryBuilder;

    /**
     * Class constructor.
     */
    public function __construct(?MapInterface $map = null)
    {
        parent::__construct($map);

        $this->queryBuilder = (new FilterQueryBuilder(new FilterComposer()))
            ->withPreparation(AssertMap::asPreparation(), new MapFilter())
            ->withFinalization(new OneOrMany());
        $this->updateQueryBuilder = (new UpdateQueryBuilder(new UpdateComposer()))
            ->withPreparation(AssertMap::asPreparation())
            ->withPreparation(new MapUpdate());
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
     * Query and update records.
     *
     * @param array                                 $filter
     * @param UpdateInstruction|UpdateInstruction[] $update
     * @param OptionInterface[]                     $opts
     * @return Result
     */
    public function update(array $filter, $update, array $opts = []): Result
    {
        if ($update instanceof UpdateInstruction) {
            $update = [$update];
        }

        $this->configureMap($opts);

        $filterQuery = new FilterQuery('update');
        $updateQuery = new UpdateQuery($filterQuery);

        $this->queryBuilder->apply($filterQuery, $filter, $opts);
        $this->updateQueryBuilder->apply($updateQuery, $update, $opts);

        $method = $updateQuery->getExpectedMethod('updateOne', 'updateMany');
        $mongoFilter = $filterQuery->toArray();
        $mongoUpdate = $updateQuery->toArray();
        $mongoOptions = $updateQuery->getOptions();

        $this->debug("%s.$method", [
            'query' => $mongoFilter,
            'update' => $mongoUpdate,
            'options' => $mongoOptions
        ]);

        /** @var UpdateResult|BulkWriteResult $writeResult */
        $writeResult = $this->getCollection()->{$method}($mongoFilter, $mongoUpdate, $mongoOptions);

        return $this->createUpdateResult($writeResult, $opts);
    }

    /**
     * @param UpdateResult|BulkWriteResult $writeResult
     * @param OptionInterface[]            $opts
     * @return Result
     */
    protected function createUpdateResult($writeResult, array $opts): Result
    {
        $meta = $writeResult->isAcknowledged() ? [
            'count' => (int)$writeResult->getModifiedCount() + $writeResult->getUpsertedCount(),
            'matched' => $writeResult->getMatchedCount(),
            'modified' => $writeResult->getModifiedCount(),
            'upserted' => $writeResult->getUpsertedCount(),
        ] : [];

        $ids = $writeResult instanceof BulkWriteResult
            ? $writeResult->getUpsertedIds()
            : [$writeResult->getUpsertedId()];

        $documents = Pipeline::with($ids)
            ->filter(fn($id) => $id !== null)
            ->map(fn($id) => ['_id' => $id]);

        return $this->resultBuilder->withOpts($opts)
            ->with($documents, $meta);
    }


    /**
     * @inheritDoc
     */
    public function save($item, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Update only writer; save is not supported");
    }

    /**
     * @inheritDoc
     */
    public function saveAll(iterable $items, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Update only writer; save is not supported");
    }

    /**
     * @inheritDoc
     */
    public function delete(array $filter, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Update only writer; delete is not supported");
    }
}
