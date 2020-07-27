<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\Functions as opt;
use Jasny\DB\Option\LookupOption;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Query\ApplyMapToFilter;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\ComposerInterface;
use Jasny\DB\Query\FilterParser;
use Jasny\DB\Schema\SchemaInterface;

/**
 * Apply a lookup option to the query as '$lookup' pipeline stage.
 */
class ApplyLookup implements ComposerInterface
{
    use LookupTrait;

    private ?ComposerInterface $lookupComposer;

    /**
     * Class constructor.
     */
    public function __construct(?ComposerInterface $lookupComposer = null)
    {
        $this->lookupComposer = $lookupComposer;
    }

    /**
     * Get the composer used to convert the lookup filter.
     */
    public function getLookupComposer(): ComposerInterface
    {
        $this->lookupComposer ??= new Composer(
            new FilterParser(),
            new ApplyMapToFilter(),
            new FilterComposer(),
            new ApplyProjection(),
            new ApplyLimit(),
            new ApplySort(),
        );

        return $this->lookupComposer;
    }


    /**
     * @inheritDoc
     */
    public function compose(object $accumulator, iterable $items, array $opts = []): void
    {
        i\iterable_walk(
            $this->apply($accumulator, $items, $opts)
        );
    }

    /**
     * @inheritDoc
     */
    public function prepare(iterable $items, array &$opts = []): iterable
    {
        return $items;
    }

    /**
     * @inheritDoc
     */
    public function finalize(object $accumulator, array $opts): void
    {
        return;
    }


    /**
     * @inheritDoc
     */
    public function apply(object $query, iterable $items, array $opts): iterable
    {
        /** @var LookupOption[] $opts */
        $lookupOpts = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof LookupOption)
            ->toArray();

        // Quick return
        if ($opts === []) {
            return $items;
        }

        /** @var FilterQuery $query */
        i\type_check($query, FilterQuery::class);

        $collection = $this->getCollectionSetting($opts);
        $schema = $this->getSchemaSetting($opts);

        foreach ($lookupOpts as $opt) {
            $this->applyOpt($query, $opt, $collection, $schema);
        }

        return $items;
    }

    /**
     * Apply a lookup option to the query.
     */
    protected function applyOpt(
        FilterQuery $query,
        LookupOption $opt,
        string $collection,
        SchemaInterface $schema
    ): void {
        $related = $opt->getRelated();
        $name = $opt->getName();

        $relationship = $schema->getRelationship($collection, $related);
        $match = $relationship->getMatch();

        $subOpts = array_merge([
            opt\setting('schema', $schema),
            opt\setting('collection', $related),
            opt\setting('map', $schema->map($related)),
        ], $opt->getOpts());

        $let = $this->calcLookupLet($match);
        $subquery = $this->lookupCompose($match, $opt->getFilter(), $subOpts);

        if ($opt->isCount()) {
            $subquery->count();
        }

        $lookup = [
            'from' => $relationship->getRelatedCollection(),
            'let' => $let,
            'pipeline' => $subquery->getPipeline(),
            'as' => $name,
        ];

        $query->addStage('$lookup', $lookup);

        if ($opt->isCount()) {
            $query->addFields([$name => ['$arrayElemAt' => ['$' . $name . '.count', 0]]]);
        } elseif (!$relationship->isToMany()) {
            $query->addFields([$name => ['$arrayElemAt' => ['$' . $name, 0]]]);
        }
    }

    /**
     * Use the filter composer to convert the lookup filter to a MongoDB expression.
     *
     * @param array<string,string>              $match
     * @param array<string,string>|FilterItem[] $filter
     * @param OptionInterface[]                 $opts
     * @return FilterQuery
     */
    protected function lookupCompose(array $match, array $filter, array $opts): FilterQuery
    {
        $query = new FilterQuery();

        $query->match(['$expr' => $this->calcMatchExpr($match)]);
        $this->getLookupComposer()->compose($query, $filter, $opts);

        return $query;
    }
}
