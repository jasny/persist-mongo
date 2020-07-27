<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Reader;

use Improved as i;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\AbstractService;
use Jasny\DB\Mongo\Query\Filter\ApplyHydrate;
use Jasny\DB\Mongo\Query\Filter\ApplyLookup;
use Jasny\DB\Mongo\Query\Filter\ApplyProjection;
use Jasny\DB\Mongo\Query\Filter\ApplyLimit;
use Jasny\DB\Mongo\Query\Filter\ApplySort;
use Jasny\DB\Mongo\Query\Filter\FilterComposer;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Mongo\Model\BSONToPHP;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Query\ApplyMapToFilter;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\FilterParser;
use Jasny\DB\Query\SetMap;
use Jasny\DB\Reader\ReadInterface;
use Jasny\DB\Result\Result;
use Jasny\Immutable;
use MongoDB\Collection;
use MongoDB\Database;

/**
 * Fetch data from a MongoDB collection
 */
class Reader extends AbstractService implements ReadInterface
{
    use Immutable\With;

    /**
     * Reader constructor.
     *
     * @param Database|Collection|null $storage
     */
    public function __construct($storage)
    {
        parent::__construct($storage);

        $this->composer = new Composer(
            new SetMap(fn(MapInterface $map) => new AssertMap($map)),
            new FilterParser(),
            new ApplyMapToFilter(),
            new ApplyHydrate(),
            new ApplyLookup(),
            new FilterComposer(),
            new ApplyProjection(),
            new ApplySort(),
            new ApplyLimit(),
        );
    }


    /**
     * Fetch the number of entities in the set.
     *
     * @param array<string,mixed>|FilterItem[] $filter
     * @param OptionInterface                  ...$opts
     * @return int
     */
    public function count(array $filter = [], OptionInterface ...$opts): int
    {
        $this->configure($opts);

        $query = new FilterQuery();
        $this->composer->compose($query, $filter, $opts);

        if ($query->isAggregate()) {
            return $this->countAggregate($query);
        }

        $this->debug("%s.countDocuments", ['filter' => $query->getFilter(), 'options' => $query->getOptions()]);

        return $this->getCollection()->countDocuments($query->getFilter(), $query->getOptions());
    }

    /**
     * Get the number of entities using an aggregation pipeline.
     */
    protected function countAggregate(FilterQuery $query): int
    {
        $query->count(); // Add $count pipeline stage

        $this->debug("%s.aggregate", ['pipeline' => $query->getPipeline(), 'options' => $query->getOptions()]);

        $cursor = $this->getCollection()->aggregate($query->getPipeline(), $query->getOptions());
        $result = i\iterable_to_array($cursor, false);

        return $result[0]['count'];
    }


    /**
     * Query and fetch data.
     *
     * @param array           $filter
     * @param OptionInterface ...$opts
     * @return Result
     */
    public function fetch(array $filter = [], OptionInterface ...$opts): Result
    {
        $this->configure($opts);

        $query = new FilterQuery();
        $this->composer->compose($query, $filter, $opts);

        $cursor = $query->isAggregate()
            ? $this->aggregate($query)
            : $this->find($query);

        return $this->resultBuilder->withOpts($opts)
            ->with($cursor)
            ->map(new BSONToPHP());
    }

    /**
     * Execute a MongoDB aggregate command.
     */
    protected function aggregate(FilterQuery $query): \Traversable
    {
        $this->debug("%s.aggregate", ['pipeline' => $query->getPipeline(), 'options' => $query->getOptions()]);

        return $this->getCollection()->aggregate($query->getPipeline(), $query->getOptions());
    }

    /**
     * Execute a MongoDB find command.
     */
    protected function find(FilterQuery $query): \Traversable
    {
        $this->debug("%s.find", ['filter' => $query->getFilter(), 'options' => $query->getOptions(true)]);

        return $this->getCollection()->find($query->getFilter(), $query->getOptions(true));
    }
}
