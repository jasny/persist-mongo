<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Map\NoMap;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\FieldsOption;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Query\ComposerInterface;

/**
 * Apply fields and omit query option(s) to a MongoDB query.
 *
 * @implements ComposerInterface<FilterQuery,FilterItem>
 */
class ApplyProjection implements ComposerInterface
{
    /**
     * @inheritDoc
     */
    public function compose(object $accumulator, iterable $items, array $opts = []): void
    {
        $this->finalize($accumulator, $opts);
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
    public function apply(object $query, iterable $items, array $opts): iterable
    {
        return $items;
    }

    /**
     * @inheritDoc
     */
    public function finalize(object $query, array $opts): void
    {
        /** @var FilterQuery $accumulator */
        i\type_check($query, FilterQuery::class);

        $projection = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof FieldsOption)
            ->map(fn(FieldsOption $opt) => $this->project($opt->getFields(), $opt->isNegated(), $opts))
            ->flatten(true)
            ->toArray();

        if ($projection === []) {
            return;
        }

        // MongoDB will include `_id` if not specified.
        if (!isset($projection['_id']) && i\iterable_has_any($projection, fn($value) => $value !== 0)) {
            $projection['_id'] = 0;
        }

        $query->project($projection);
    }

    /**
     * Convert fields/omit opt to MongoDB projection option.
     *
     * @param string[]          $fields
     * @param bool              $negate
     * @param OptionInterface[] $opts
     * @return iterable<string,int>
     */
    protected function project(array $fields, bool $negate, array $opts): iterable
    {
        $map = opts\setting('map', new NoMap())->findIn($opts, MapInterface::class);

        return Pipeline::with($fields)
            ->typeCheck('string', new \UnexpectedValueException())
            ->flip()
            ->fill($negate ? 0 : 1)
            ->mapKeys(fn($_, string $field) => $map->applyToField($field) ?? $field);
    }
}
