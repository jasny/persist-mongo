<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Writer;

use Improved as i;
use Jasny\DB\Exception\UnsupportedFeatureException;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\AbstractService;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Mongo\Query\Save\SaveComposer;
use Jasny\DB\Mongo\Query\WriteQuery;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Query\ApplyMapToItems;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\SetMap;
use Jasny\DB\Result\Result;
use Jasny\DB\Writer\WriteInterface;
use MongoDB\BulkWriteResult;

/**
 * Save data to a MongoDB collection.
 *
 * @template TItem
 * @extends AbstractService<SaveQuery,TItem>
 * @implements WriteInterface<TItem>
 */
class Save extends AbstractService implements WriteInterface
{
    /**
     * Class constructor.
     */
    public function __construct(?MapInterface $map = null)
    {
        parent::__construct($map);

        $this->composer = new Composer(
            new SetMap(fn(MapInterface $map) => new AssertMap($map)),
            new ApplyMapToItems(),
            new SaveComposer(),
        );
    }


    /**
     * @inheritDoc
     */
    public function save($item, array $opts = []): Result
    {
        return $this->saveAll([$item], $opts);
    }

    /**
     * @inheritDoc
     */
    public function saveAll(iterable $items, array $opts = []): Result
    {
        $this->configureMap($opts);

        $applyResult = opts\apply_result()->isIn($opts);
        $items = $applyResult ? i\iterable_to_array($items) : $items;

        $query = new WriteQuery(['ordered' => false]);
        $this->composer->compose($query, $items, $opts);

        $query->expectMethods('insertOne', 'replaceOne', 'updateOne');

        $this->debug("%s.bulkWrite", ['operations' => $query->getOperations(), 'options' => $query->getOptions()]);

        $writeResult = $this->getCollection()->bulkWrite($query->getOperations(), $query->getOptions());

        $result = $this->createSaveResult($query->getIndex(), $writeResult, $opts);

        if ($applyResult) {
            /** @var array $items */
            $result = $result->applyTo($items);
        }

        return $result;
    }

    /**
     * Aggregate the meta from multiple bulk write actions.
     *
     * @param array             $index
     * @param BulkWriteResult   $writeResult
     * @return Result<\stdClass>
     */
    protected function createSaveResult(array $index, BulkWriteResult $writeResult, array $opts): Result
    {
        $meta = [];

        if ($writeResult->isAcknowledged()) {
            $meta['count'] = $writeResult->getInsertedCount()
                + (int)$writeResult->getModifiedCount()
                + $writeResult->getUpsertedCount();
            $meta['matched'] = $writeResult->getMatchedCount();
            $meta['inserted'] = $writeResult->getInsertedCount();
            $meta['modified'] = $writeResult->getModifiedCount();
        }

        $ids = $writeResult->getInsertedIds()
            + $writeResult->getUpsertedIds()
            + array_fill(0, count($index), null);

        // Turn id values into arrays before mapping is applied.
        $documents = i\iterable_map($ids, fn($id) => (object)($id === null ? [] : ['_id' => $id]));

        return $this->resultBuilder->withOpts($opts)
            ->with($documents, $meta)
            ->setKeys($index);
    }


    /**
     * @inheritDoc
     * @throws UnsupportedFeatureException
     */
    public function update(array $filter, $instructions, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Save only writer; update is not supported");
    }

    /**
     * @inheritDoc
     * @throws UnsupportedFeatureException
     */
    public function delete(array $filter, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Save only writer; delete is not supported");
    }
}
