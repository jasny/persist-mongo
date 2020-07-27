<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Map\NoMap;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Option\SortOption;
use Jasny\DB\Query\ComposerInterface;

/**
 * Apply sort query option(s) to a MongoDB query.
 *
 * @implements ComposerInterface<FilterQuery,FilterItem>
 */
class ApplySort implements ComposerInterface
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
     * Convert sort opt to MongoDB sort option.
     *
     * @param string[]          $fields
     * @param OptionInterface[] $opts
     * @return array<string,int>
     */
    protected function convert(array $fields, array $opts): array
    {
        /** @var MapInterface $map */
        $map = opts\setting('map', new NoMap())->findIn($opts, MapInterface::class);

        return Pipeline::with($fields)
            ->typeCheck('string', new \UnexpectedValueException())
            ->flip()
            ->map(fn($_, string $field) => ($field[0] === '~' ? -1 : 1))
            ->mapKeys(fn(int $asc, string $field) => ($asc < 0 ? substr($field, 1) : $field))
            ->mapKeys(fn($_, string $field) => $map->applyToField($field) ?? $field)
            ->toArray();
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
        $sort = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof SortOption)
            ->map(fn(SortOption $option) => $this->convert($option->getFields(), $opts))
            ->flatten(true)
            ->toArray();

        if ($sort === []) {
            return;
        }

        /** @var FilterQuery $accumulator */
        i\type_check($query, FilterQuery::class);

        $query->sort($sort);
    }
}
