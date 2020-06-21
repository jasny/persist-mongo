<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Reader;

use Jasny\DB\Map\MapInterface;
use Jasny\DB\Filter\Prepare\MapFilter;
use Jasny\DB\Mongo\AbstractService;
use Jasny\DB\Mongo\Filter\FilterComposer;
use Jasny\DB\Mongo\Filter\Finalize\ApplyFields;
use Jasny\DB\Mongo\Filter\Finalize\ApplyLimit;
use Jasny\DB\Mongo\Filter\Finalize\ApplySort;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Mongo\Model\BSONToPHP;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\QueryBuilder\FilterQueryBuilder;
use Jasny\DB\Reader\ReadInterface;
use Jasny\DB\Result\Result;
use Jasny\Immutable;

/**
 * Fetch data from a MongoDB collection
 */
class Reader extends AbstractService implements ReadInterface
{
    use Immutable\With;

    /**
     * Reader constructor.
     */
    public function __construct(?MapInterface $map = null)
    {
        parent::__construct($map);

        $this->queryBuilder = (new FilterQueryBuilder(new FilterComposer()))
            ->withPreparation(AssertMap::asPreparation(), new MapFilter())
            ->withFinalization(new ApplyFields(), new ApplySort(), new ApplyLimit());
    }


    /**
     * Fetch the number of entities in the set.
     *
     * @param array             $filter
     * @param OptionInterface[] $opts
     * @return int
     */
    public function count(array $filter = [], array $opts = []): int
    {
        $this->configureMap($opts);

        $query = new FilterQuery('countDocuments');
        $this->queryBuilder->apply($query, $filter, $opts);

        $method = $query->getExpectedMethod('countDocuments', 'estimatedDocumentCount');
        $mongoFilter = $query->toArray();
        $mongoOptions = $query->getOptions();

        $this->debug("%s.$method", ['filter' => $mongoFilter, 'options' => $mongoOptions]);

        return $method === 'estimatedDocumentCount'
            ? $this->getCollection()->estimatedDocumentCount($mongoOptions)
            : $this->getCollection()->countDocuments($mongoFilter, $mongoOptions);
    }

    /**
     * Query and fetch data.
     *
     * @param array             $filter
     * @param OptionInterface[] $opts
     * @return Result
     */
    public function fetch(array $filter = [], array $opts = []): Result
    {
        $this->configureMap($opts);

        $query = new FilterQuery('find');
        $this->queryBuilder->apply($query, $filter, $opts);

        $method = $query->getExpectedMethod('find', 'aggregate');
        $mongoFilter = $query->toArray();
        $mongoOptions = $query->getOptions();

        $this->debug("%s.$method", [
            ($method === 'aggregate' ? 'pipeline' : 'filter') => $mongoFilter,
            'options' => $mongoOptions
        ]);

        $cursor = $method === 'find'
            ? $this->getCollection()->find($mongoFilter, $mongoOptions)
            : $this->getCollection()->aggregate($mongoFilter, $mongoOptions);

        return $this->resultBuilder->withOpts($opts)
            ->with($cursor)
            ->map(new BSONToPHP());
    }
}
