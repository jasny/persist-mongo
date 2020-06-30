<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Writer;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Exception\UnsupportedFeatureException;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\AbstractService;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Mongo\Query\Filter\FilterComposer;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Mongo\Query\Update\OneOrMany;
use Jasny\DB\Mongo\Query\Update\UpdateComposer;
use Jasny\DB\Mongo\Query\UpdateQuery;
use Jasny\DB\Option\LimitOption;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Query\ApplyMapToFilter;
use Jasny\DB\Query\ApplyMapToUpdate;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\ComposerInterface;
use Jasny\DB\Query\FilterParser;
use Jasny\DB\Query\SetMap;
use Jasny\DB\Result\Result;
use Jasny\DB\Update\UpdateInstruction;
use Jasny\DB\Writer\WriteInterface;
use Jasny\Immutable;
use MongoDB\BulkWriteResult;
use MongoDB\UpdateResult;

/**
 * Update data of a MongoDB collection.
 *
 * @extends AbstractService<FilterQuery,FilterItem>
 */
class Update extends AbstractService implements WriteInterface
{
    use Immutable\With;

    /** @phpstan-var ComposerInterface<UpdateQuery,UpdateInstruction> */
    protected ComposerInterface $updateComposer;

    /**
     * Class constructor.
     */
    public function __construct(?MapInterface $map = null)
    {
        parent::__construct($map);

        $this->composer = new Composer(
            new FilterParser(),
            new SetMap(fn(MapInterface $map) => new AssertMap($map)),
            new ApplyMapToFilter(),
            new FilterComposer(),
        );

        $this->updateComposer = new Composer(
            new SetMap(fn(MapInterface $map) => new AssertMap($map)),
            new ApplyMapToUpdate(),
            new UpdateComposer(),
            new OneOrMany(),
        );
    }

    /**
     * Get the update query builder used by this service.
     */
    public function getUpdateComposer(): ComposerInterface
    {
        return $this->updateComposer;
    }

    /**
     * Get a copy with a different query builder.
     *
     * @param ComposerInterface $composer
     * @return static
     */
    public function withUpdateComposer(ComposerInterface $composer): self
    {
        return $this->withProperty('updateComposer', $composer);
    }


    /**
     * Query and update records.
     *
     * @param array<string,mixed>|FilterItem[]              $filter
     * @param UpdateInstruction|iterable<UpdateInstruction> $update
     * @param OptionInterface[]                             $opts
     * @return Result
     */
    public function update(array $filter, $update, array $opts = []): Result
    {
        if ($update instanceof UpdateInstruction) {
            $update = [$update];
        }

        $this->configureMap($opts);

        $filterQuery = new FilterQuery();
        $updateQuery = new UpdateQuery($filterQuery);

        $this->composer->compose($filterQuery, $filter, $opts);
        $this->updateComposer->compose($updateQuery, $update, $opts);

        $method = i\iterable_has_any($opts, fn($opt) => $opt instanceof LimitOption) ? 'updateOne' : 'updateMany';

        $this->debug("%s.$method", [
            'query' => $filterQuery->getFilter(),
            'update' => $updateQuery->getUpdate(),
            'options' => $updateQuery->getOptions(),
        ]);

        /** @var UpdateResult|BulkWriteResult $writeResult */
        $writeResult = $this->getCollection()->{$method}(
            $filterQuery->getFilter(),
            $updateQuery->getUpdate(),
            $updateQuery->getOptions()
        );

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
