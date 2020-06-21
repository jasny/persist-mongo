<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Filter\Finalize;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Map\NoMap;
use Jasny\DB\Mongo\Query\QueryInterface;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Option\SortOption;

/**
 * Convert sort query option to a MongoDB query.
 */
class ApplySort
{
    /**
     * Apply sort opt to a MongoDB query.
     *
     * @param QueryInterface    $query
     * @param OptionInterface[] $opts
     */
    public function __invoke(QueryInterface $query, array $opts): void
    {
        $sort = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof SortOption)
            ->map(fn(SortOption $option) => $this->convert($option->getFields(), $opts))
            ->flatten(true)
            ->toArray();

        if ($sort !== []) {
            $query->setOption('sort', $sort);
        }
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
}
