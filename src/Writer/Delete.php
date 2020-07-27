<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Writer;

use Improved as i;
use Jasny\DB\Exception\UnsupportedFeatureException;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\AbstractService;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Mongo\Query\Filter\FilterComposer;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Mongo\Query\Update\OneOrMany;
use Jasny\DB\Option\LimitOption;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Query\ApplyMapToFilter;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\SetMap;
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
    public function __construct()
    {
        parent::__construct();

        $this->composer = new Composer(
            new SetMap(fn(MapInterface $map) => new AssertMap($map)),
            new ApplyMapToFilter(),
            new FilterComposer(),
            new OneOrMany(),
        );
    }

    /**
     * Query and delete records.
     * The result will not contain any items, only meta data `count` with the number of deleted items.
     *
     * @param array<string,mixed>|FilterItem[] $filter
     * @param OptionInterface                  ...$opts
     * @return Result
     */
    public function delete(array $filter, OptionInterface ...$opts): Result
    {
        $this->configure($opts);

        $query = new FilterQuery();
        $this->composer->compose($query, $filter, $opts);

        $method = i\iterable_has_any($opts, fn($opt) => $opt instanceof LimitOption) ? 'deleteOne' : 'deleteMany';

        $this->debug("%s.$method", ['filter' => $query->getFilter(), 'options' => $query->getOptions()]);

        $deleteResult = $this->getStorage()->{$method}($query->getFilter(), $query->getOptions());

        $meta = $deleteResult->isAcknowledged() ? ['count' => $deleteResult->getDeletedCount()] : [];

        return $this->resultBuilder->withOpts($opts)
            ->with([], $meta);
    }


    /**
     * @inheritDoc
     */
    public function save($item, OptionInterface ...$opts): Result
    {
        throw new UnsupportedFeatureException("Delete only writer; save is not supported");
    }

    /**
     * @inheritDoc
     */
    public function saveAll(iterable $items, OptionInterface ...$opts): Result
    {
        throw new UnsupportedFeatureException("Delete only writer; save is not supported");
    }

    /**
     * @inheritDoc
     */
    public function update(array $filter, $instructions, OptionInterface ...$opts): Result
    {
        throw new UnsupportedFeatureException("Delete only writer; update is not supported");
    }
}
