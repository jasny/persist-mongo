<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Query\ComposerInterface;

/**
 * Standard logic to apply a filter item to a query.
 *
 * @implements ComposerInterface<FilterQuery,FilterItem>
 */
class FilterComposer implements ComposerInterface
{
    /**
     * Default operator conversion
     */
    protected const OPERATORS = [
        '' => null,
        'not' => '$ne',
        'min' => '$gte',
        'max' => '$lte',
        'any' => '$in',
        'none' => '$nin',
        'all' => '$all'
    ];

    /**
     * @inheritDoc
     */
    public function compose(object $query, iterable $items, array $opts = []): void
    {
        i\iterable_walk(
            $this->apply($query, $items, $opts)
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
    public function apply(object $query, iterable $items, array $opts): iterable
    {
        /** @var FilterQuery $query */
        i\type_check($query, FilterQuery::class);

        foreach ($items as $item) {
            $applied = $this->applyItem($query, $item);

            if (!$applied) {
                yield $item;
            }
        }
    }

    /**
     * Apply a filter item to the query.
     */
    public function applyItem(FilterQuery $query, FilterItem $filterItem): bool
    {
        [$field, $operator, $value] = [$filterItem->getField(), $filterItem->getOperator(), $filterItem->getValue()];

        if (!array_key_exists($operator, static::OPERATORS)) {
            return false;
        }

        $mongoOperator = static::OPERATORS[$operator];
        $condition = $mongoOperator !== null ? [$field => [$mongoOperator => $value]] : [$field => $value];

        $query->match($condition);

        return true;
    }

    /**
     * @inheritDoc
     */
    public function finalize(object $query, array $opts): void
    {
    }
}
