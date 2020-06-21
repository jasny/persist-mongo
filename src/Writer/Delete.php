<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Writer;

use Jasny\DB\Exception\UnsupportedFeatureException;
use Jasny\DB\Filter\Prepare\MapFilter;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\AbstractService;
use Jasny\DB\Mongo\Filter\FilterComposer;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Mongo\QueryBuilder\Finalize\OneOrMany;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\QueryBuilder\FilterQueryBuilder;
use Jasny\DB\Result\Result;
use Jasny\DB\Writer\WriteInterface;

/**
 * Delete data from a MongoDB collection.
 */
class Delete extends AbstractService implements WriteInterface
{
    /**
     * Class constructor.
     */
    public function __construct(?MapInterface $map = null)
    {
        parent::__construct($map);

        $this->queryBuilder = (new FilterQueryBuilder(new FilterComposer()))
            ->withPreparation(AssertMap::asPreparation(), new MapFilter())
            ->withFinalization(new OneOrMany());
    }

    /**
     * Query and delete records.
     * The result will not contain any items, only meta data `count` with the number of deleted items.
     *
     * @param array             $filter
     * @param OptionInterface[] $opts
     * @return Result
     */
    public function delete(array $filter, array $opts = []): Result
    {
        $this->configureMap($opts);

        $query = new FilterQuery('delete');
        $this->queryBuilder->apply($query, $filter, $opts);

        $method = $query->getExpectedMethod('deleteOne', 'deleteMany');
        $mongoFilter = $query->toArray();
        $mongoOptions = $query->getOptions();

        $this->debug("%s.$method", ['filter' => $mongoFilter, 'options' => $mongoOptions]);

        $deleteResult = $method === 'deleteOne'
            ? $this->getStorage()->deleteOne($mongoFilter, $mongoOptions)
            : $this->getStorage()->deleteMany($mongoFilter, $mongoOptions);

        $meta = $deleteResult->isAcknowledged() ? ['count' => $deleteResult->getDeletedCount()] : [];

        return $this->resultBuilder->withOpts($opts)
            ->with([], $meta);
    }


    /**
     * @inheritDoc
     */
    public function save($item, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Delete only writer; save is not supported");
    }

    /**
     * @inheritDoc
     */
    public function saveAll(iterable $items, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Delete only writer; save is not supported");
    }

    /**
     * @inheritDoc
     */
    public function update(array $filter, $instructions, array $opts = []): Result
    {
        throw new UnsupportedFeatureException("Delete only writer; update is not supported");
    }
}
