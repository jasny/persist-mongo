<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\LimitOption;
use Jasny\DB\Query\ComposerInterface;

/**
 * Apply limit query option to a MongoDB query.
 * If multiple limit options are present, only the last is used.
 *
 * @implements ComposerInterface<FilterQuery,FilterItem>
 */
class ApplyLimit implements ComposerInterface
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

        /** @var LimitOption|null $limitOpt */
        $limitOpt = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof LimitOption)
            ->last();

        if ($limitOpt === null) {
            return;
        }

        $query->limit($limitOpt->getLimit());
        $query->skip($limitOpt->getOffset());
    }
}
