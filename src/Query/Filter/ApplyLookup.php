<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\Functions as opt;
use Jasny\DB\Option\HydrateOption;
use Jasny\DB\Option\LookupOption;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Query\ApplyMapToFilter;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\ComposerInterface;
use Jasny\DB\Query\FilterParser;
use Jasny\DB\Schema\Relationship;
use Jasny\DB\Schema\SchemaInterface;

/**
 * Apply a lookup and hydrate options to the query as '$lookup' pipeline stage.
 *
 * @implements ComposerInterface<FilterQuery,FilterItem>
 */
class ApplyLookup implements ComposerInterface
{
    protected ComposerInterface $lookupComposer;

    /**
     * Class constructor.
     */
    public function __construct(?ComposerInterface $lookupComposer = null)
    {
        if ($lookupComposer !== null) {
            $this->lookupComposer = new Composer(
                $lookupComposer,
                $this, // recursion for nested lookup
            );
        }
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
            $this, // recursion for nested lookup
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
    }


    /**
     * Apply items to given query.
     *
     * @param FilterQuery&object   $query  Database specific query object.
     * @param iterable<FilterItem> $items
     * @param OptionInterface[]    $opts
     * @return iterable<FilterItem>
     */
    public function apply(object $query, iterable $items, array $opts): iterable
    {
        $collection = null;
        $schema = null;

        foreach ($opts as $opt) {
            if (!$opt instanceof LookupOption && !$opt instanceof HydrateOption) {
                continue;
            }

            $collection ??= $this->getSetting('collection', $opts);
            $schema ??= $this->getSetting('schema', $opts);

            $this->applyOpt($query, $opt, $collection, $schema);
        }

        return $items;
    }

    /**
     * Get a setting from opts.
     *
     * @param string            $name
     * @param OptionInterface[] $opts
     * @param string|null       $type
     * @return mixed
     * @throws \LogicException if setting isn't present.
     *
     * @template T
     * @phpstan-param string               $name
     * @phpstan-param OptionInterface[]    $opts
     * @phpstan-param class-string<T>|null $type
     * @phpstan-return T
     */
    protected function getSetting(string $name, array $opts, ?string $type = null)
    {
        $value = opt\setting($name, null)->findIn($opts, $type);

        if ($value === null) {
            throw new \LogicException("Failed to apply lookup; missing '$name' setting");
        }

        return $value;
    }

    /**
     * Apply a lookup option to the query.
     *
     * @param FilterQuery                $query
     * @param LookupOption|HydrateOption $opt
     * @param string                     $collection
     * @param SchemaInterface            $schema
     *
     * @todo Get the nested map, instead of getting a map from the schema.
     */
    protected function applyOpt(
        FilterQuery $query,
        $opt,
        string $collection,
        SchemaInterface $schema
    ): void {
        $relationship = $opt instanceof HydrateOption
            ? $schema->getRelationshipForField($collection, $opt->getField())
            : $schema->getRelationship($collection, $opt->getRelated());

        $name = $opt->getName();
        $match = $relationship->getMatch();
        $related = $relationship->getRelatedCollection();

        $filter = $opt instanceof LookupOption ? $opt->getFilter() : [];
        $isCount = $opt instanceof LookupOption && $opt->isCount();

        $subOpts = $opt->getOpts();
        $subOpts[] = opt\setting('schema', $schema);
        $subOpts[] = opt\setting('collection', $related);
        $subOpts[] = opt\setting('map', $schema->map($related));

        $let = $this->calcLookupLet($match);
        $subQuery = $this->lookupCompose($match, $filter, $subOpts);

        if ($isCount) {
            $subQuery->count();
        }

        $lookup = [
            'from' => $related,
            'let' => $let,
            'pipeline' => $subQuery->getPipeline(),
            'as' => $name,
        ];

        $query->addStage('$lookup', $lookup);

        if ($isCount) {
            $query->addFields([$name => ['$arrayElemAt' => ['$' . $name . '.count', 0]]]);
        } elseif (!$relationship->isToMany()) {
            $query->addFields([$name => ['$arrayElemAt' => ['$' . $name, 0]]]);
        }

        if ($opt instanceof HydrateOption && $opt->getName() !== $opt->getField()) {
            $query->project([$opt->getField() => 0]);
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

    /**
     * Calculate the `let` property for a complex lookup.
     *
     * @param array<string,string> $match
     * @return array<string,string>
     */
    private function calcLookupLet(array $match): array
    {
        $let = [];

        foreach (array_keys($match) as $field) {
            $let[strtolower($field)] = '$' . $field;
        }

        return $let;
    }

    /**
     * Calculate the match expression for the lookup pipeline.
     *
     * @param array<string,string> $match
     */
    private function calcMatchExpr(array $match): array
    {
        $expr = [];

        foreach ($match as $field => $var) {
            $expr[] = ['$eq' =>  ['$' . $field, '$$'  . strtolower($var)]];
        }

        return count($expr) === 1 ? $expr[0] : ['$and' => $expr];
    }
}
